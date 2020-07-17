package kafka

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"time"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

// Consumer represents a new Kafka cluster consumer.
type Consumer struct {
	printer                 internal.Printer
	internalConsumer        partitionConsumer
	store                   offsetStore
	remoteTopics            []string
	enableAutoTopicCreation bool
	events                  chan *Event
	closeOnce               sync.Once
	localOffsets            TopicPartitionOffset
	exclusive               bool
	idleTimeout             time.Duration
	wg                      sync.WaitGroup
}

// NewConsumer creates a new instance of Kafka cluster consumer.
func NewConsumer(
	store offsetStore,
	internalConsumer partitionConsumer,
	printer internal.Printer,
	enableAutoTopicCreation bool,
	exclusive bool,
	idleTimeout time.Duration) (*Consumer, error) {

	return &Consumer{
		printer:                 printer,
		internalConsumer:        internalConsumer,
		enableAutoTopicCreation: enableAutoTopicCreation,
		events:                  make(chan *Event, 128),
		store:                   store,
		localOffsets:            make(TopicPartitionOffset),
		exclusive:               exclusive,
		idleTimeout:             idleTimeout,
	}, nil
}

// GetTopics fetches the topics from the server.
func (c *Consumer) GetTopics(search *regexp.Regexp) ([]string, error) {
	if c.remoteTopics != nil {
		return c.remoteTopics, nil
	}

	topics, err := c.internalConsumer.Topics()
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the topic list from the server: %w", err)
	}

	c.remoteTopics = make([]string, 0)
	for _, topic := range topics {
		if search == nil {
			c.remoteTopics = append(c.remoteTopics, topic)
			continue
		}
		if search.Match([]byte(topic)) {
			c.remoteTopics = append(c.remoteTopics, topic)
		}
	}

	return c.remoteTopics, nil
}

// Events the channel to which the Kafka events will be published.
//
// You MUST listen to this channel before you start the consumer to avoid deadlock.
func (c *Consumer) Events() <-chan *Event {
	return c.events
}

// Start starts consuming from the specified topics and executes the callback function on each message.
//
// This is a blocking call which will be terminated on cancellation of the context parameter.
// The method returns error if the topic list is empty or the callback function is nil.
func (c *Consumer) Start(ctx context.Context, topics map[string]*PartitionCheckpoints) error {
	if len(topics) == 0 {
		return errors.New("the topic list cannot be empty")
	}

	go func() {
		for err := range c.store.errors() {
			c.printer.Errorf(internal.Forced, "Offset Storage Error: %s", err)
		}
	}()

	c.printer.Infof(internal.VeryVerbose, "Starting Kafka consumers.")
	topicPartitionOffsets, err := c.fetchTopicPartitions(topics)
	if err != nil {
		return err
	}
	c.store.start(topicPartitionOffsets)

	c.consumeTopics(ctx, topicPartitionOffsets)
	return nil
}

// StoreOffset stores the offset of the successfully processed message into the offset store.
func (c *Consumer) StoreOffset(event *Event) {
	err := c.store.commit(event.Topic, event.Partition, event.Offset+1)
	if err != nil {
		c.printer.Errorf(internal.Forced, "Failed to commit the offset: %s.", err)
	}
}

// CloseOffsetStore closes the underlying offset store.
//
// Make sure you call this function once you processed all the messages.
func (c *Consumer) CloseOffsetStore() {
	c.store.close()
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.printer.Info(internal.Verbose, "Closing Kafka consumer.")
		err := c.internalConsumer.Close()
		if err != nil {
			c.printer.Errorf(internal.Forced, "Failed to close the Kafka consumer: %s.", err)
		} else {
			c.printer.Info(internal.VeryVerbose, "The Kafka client has been closed successfully.")
		}
		close(c.events)
	})
}

func (c *Consumer) consumeTopics(ctx context.Context, topicPartitionOffsets TopicPartitionOffset) {
	parentCtx, cancelAllPartitionConsumers := context.WithCancel(ctx)
	defer cancelAllPartitionConsumers()
	var cancelled bool
	for topic, partitionOffsets := range topicPartitionOffsets {
		t := topic
		if cancelled {
			break
		}
		select {
		case <-ctx.Done():
			cancelled = true
			break
		default:
			for partition, offset := range partitionOffsets {
				p := partition
				o := offset
				if cancelled {
					break
				}
				select {
				case <-parentCtx.Done():
					cancelled = true
					break
				default:
					c.wg.Add(1)
					go func() {
						err := c.consumePartition(parentCtx, t, p, o)
						if err != nil {
							c.printer.Errorf(internal.Forced, "Failed to start consuming from %v offset of topic %s, partition %d: %s", getOffsetString(offset.Current), t, p, err)
							cancelAllPartitionConsumers()
							cancelled = true
						}
					}()
				}
			}
		}
	}
	c.wg.Wait()
	c.Close()
}

func (c *Consumer) consumePartition(ctx context.Context, topic string, partition int32, offset Offset) error {
	defer c.wg.Done()
	var stopAt string

	if offset.stopAt != nil {
		stopAt = ", Stopping at: " + offset.stopAt.String()
	}
	c.printer.Infof(internal.VeryVerbose, "Consuming from Topic: %s, Partition: %d, Offset: %v%s", topic, partition, getOffsetString(offset.Current), stopAt)
	pc, err := c.internalConsumer.ConsumePartition(topic, partition, offset.Current)
	if err != nil {
		return err
	}

	shutdown := func(reason shutdownReason) error {
		c.printer.Infof(internal.VeryVerbose, "Closing consumer Topic: %s, Partition %d, Reason: %s", topic, partition, reason.String())
		err := pc.Close()
		if err != nil {
			return fmt.Errorf("failed to close %s consumer of partition %d: %s", topic, partition, err)
		}
		return nil
	}

	mustStop := func(m *sarama.ConsumerMessage) bool {
		if offset.stopAt == nil {
			return false
		}
		if offset.stopAt.mode == timestampMode {
			return m.Timestamp.After(offset.stopAt.at)
		}
		return m.Offset >= offset.stopAt.offset
	}

	lastMessageReceivedAt := time.Now()
	forceClose := make(chan interface{})
	if c.idleTimeout > 0 {
		go func() {
			ticker := time.NewTicker(1 * time.Second)
			defer ticker.Stop()
			for now := range ticker.C {
				if now.Sub(lastMessageReceivedAt) > c.idleTimeout {
					close(forceClose)
					return
				}
			}
		}()
	}

	for {
		select {
		case <-forceClose:
			return shutdown(noMoreMessage)
		case <-ctx.Done():
			return shutdown(cancelledByUser)
		case m, more := <-pc.Messages():
			if !more {
				return nil
			}

			lastMessageReceivedAt = time.Now()

			c.events <- &Event{
				Topic:     m.Topic,
				Key:       m.Key,
				Value:     m.Value,
				Timestamp: m.Timestamp,
				Partition: m.Partition,
				Offset:    m.Offset,
			}
			if mustStop(m) {
				return shutdown(reachedStopCheckpoint)
			}
		case err, more := <-pc.Errors():
			if !more {
				return nil
			}
			c.printer.Errorf(internal.Forced, "Failed to consume message: %s.", err)
		}
	}
}

func (c *Consumer) fetchTopicPartitions(topics map[string]*PartitionCheckpoints) (TopicPartitionOffset, error) {
	existing := make(map[string]interface{})
	if !c.enableAutoTopicCreation {
		// We need to check if the requested topic(s) exist on the server
		// That's why we need to get the list of the existing topics from the brokers.
		remote, err := c.GetTopics(nil)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the topic list from the broker(s): %w", err)
		}
		for _, t := range remote {
			existing[t] = nil
		}
	}

	topicPartitionOffsets := make(TopicPartitionOffset)

	for topic, checkpoints := range topics {
		if !c.enableAutoTopicCreation {
			if _, ok := existing[topic]; !ok {
				return nil, fmt.Errorf("failed to find the topic %s on the server. You must create the topic manually or enable automatic topic creation both on the server and in trubka", topic)
			}
		}
		offsets, err := c.calculateStartingOffsets(topic, checkpoints)
		if err != nil {
			return nil, err
		}
		topicPartitionOffsets[topic] = offsets
	}
	return topicPartitionOffsets, nil
}

func (c *Consumer) calculateStartingOffsets(topic string, checkpoints *PartitionCheckpoints) (PartitionOffset, error) {
	c.printer.Infof(internal.SuperVerbose, "Fetching partitions for topic %s.", topic)
	partitions, err := c.internalConsumer.Partitions(topic)
	if err != nil {
		return nil, fmt.Errorf("failed to fetch the partition offsets for topic %s: %w", topic, err)
	}

	offsets := make(PartitionOffset)
	for _, partition := range partitions {
		var (
			offset *Offset
			err    error
		)
		cp := checkpoints.get(partition)
		if cp == nil {
			c.printer.Infof(internal.SuperVerbose, "Partition %d of topic %s has been excluded by the user", partition, topic)
			continue
		}
		switch cp.from.mode {
		case explicitMode:
			offset, err = c.getExplicitOffset(topic, partition, cp.from)
		case timestampMode:
			offset, err = c.getTimeBasedOffset(topic, partition, cp.from)
		case localMode:
			offset, err = c.getLocalOffset(topic, partition, cp.from)
		default:
			offset = &Offset{Current: cp.from.offset}
		}
		if err != nil {
			// It only occurs when the requested explicit start offset is greater than the latest available offset
			// of the partition. If we start consuming from the partition by that offset, we will get an error from Kafka.
			// Remember that the specified offset might be valid for other partitions. So we simply ignore the out of range ones.
			if errors.Is(err, errOutOfRangeOffset) {
				continue
			}
			return nil, err
		}
		offset.stopAt = cp.to
		offsets[partition] = *offset
	}
	return offsets, nil
}

func (c *Consumer) getLocalOffset(topic string, partition int32, startFrom *checkpoint) (*Offset, error) {
	localOffsets, ok := c.localOffsets[topic]
	if !ok {
		offsets, err := c.store.read(topic)
		if err != nil {
			return nil, err
		}
		c.localOffsets[topic] = offsets
		localOffsets = offsets
	}

	latest, err := c.internalConsumer.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the current offset value for partition %d of topic %s: %w", partition, topic, err)
	}

	c.printer.Infof(internal.SuperVerbose,
		"Checking the local offset for Topic: %s, Partition: %d",
		topic,
		partition)

	if storedOffset, ok := localOffsets[partition]; ok {
		c.printer.Infof(internal.SuperVerbose,
			"Topic: %s, Partition %d, Latest Offset: %v, Start From Local: %v",
			topic,
			partition,
			getOffsetString(latest),
			getOffsetString(storedOffset.Current))

		storedOffset.Latest = latest
		return &storedOffset, nil
	}

	c.printer.Infof(internal.SuperVerbose,
		"No local offsets found for Topic: %s, Partition: %d. Latest: %v, Start From: %v",
		topic,
		partition,
		getOffsetString(latest),
		getOffsetString(startFrom.offset),
	)

	return &Offset{
		Latest:  latest,
		Current: startFrom.offset,
	}, nil
}

func (c *Consumer) getTimeBasedOffset(topic string, partition int32, startFrom *checkpoint) (*Offset, error) {
	offset, err := c.internalConsumer.GetOffset(topic, partition, startFrom.offset)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the time-based offset for partition %d of topic %s: %w", partition, topic, err)
	}
	c.printer.Infof(internal.SuperVerbose,
		"Topic: %s, Partition %d, Requested: %s, Start From: %v",
		topic,
		partition,
		startFrom.String(),
		getOffsetString(offset))
	return &Offset{
		Current: offset,
	}, nil
}

func (c *Consumer) getExplicitOffset(topic string, partition int32, startFrom *checkpoint) (*Offset, error) {
	latest, err := c.internalConsumer.GetOffset(topic, partition, sarama.OffsetNewest)
	if err != nil {
		return nil, fmt.Errorf("failed to retrieve the current offset value for partition %d of topic %s: %w", partition, topic, err)
	}
	if startFrom.offset > latest {
		c.printer.Infof(internal.SuperVerbose,
			"Topic: %s, Partition %d, Requested: %s, Available: %v, Ignored",
			topic,
			partition,
			startFrom.String(),
			getOffsetString(latest))
		return nil, errOutOfRangeOffset
	}
	c.printer.Infof(internal.SuperVerbose,
		"Topic: %s, Partition %d, Latest: %v, Start From: %v",
		topic,
		partition,
		getOffsetString(latest),
		startFrom.String())

	return &Offset{
		Current: startFrom.offset,
		Latest:  latest,
	}, nil
}

func getOffsetString(offset int64) interface{} {
	switch offset {
	case sarama.OffsetOldest:
		return "oldest"
	case sarama.OffsetNewest:
		return "newest"
	default:
		return offset
	}
}
