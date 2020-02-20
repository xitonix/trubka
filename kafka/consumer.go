package kafka

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"sync"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

// Consumer represents a new Kafka cluster consumer.
type Consumer struct {
	brokers                 []string
	printer                 internal.Printer
	internalConsumer        sarama.Consumer
	internalClient          sarama.Client
	wg                      sync.WaitGroup
	remoteTopics            []string
	enableAutoTopicCreation bool
	environment             string
	events                  chan *Event
	closeOnce               sync.Once
	store                   *offsetStore
}

// NewConsumer creates a new instance of Kafka cluster consumer.
func NewConsumer(brokers []string, printer internal.Printer, environment string, enableAutoTopicCreation bool, options ...Option) (*Consumer, error) {
	client, err := initClient(brokers, options...)
	if err != nil {
		return nil, err
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise the Kafka consumer: %w", err)
	}

	store, err := newOffsetStore(printer, environment)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		brokers:                 brokers,
		printer:                 printer,
		internalConsumer:        consumer,
		internalClient:          client,
		enableAutoTopicCreation: enableAutoTopicCreation,
		environment:             environment,
		events:                  make(chan *Event, 128),
		store:                   store,
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
	defer c.store.close()

	c.consumeTopics(ctx, topicPartitionOffsets)
	return nil
}

// StoreOffset stores the offset of the successfully processed message into the offset store.
func (c *Consumer) StoreOffset(event *Event) {
	err := c.store.Store(event.Topic, event.Partition, event.Offset+1)
	if err != nil {
		c.printer.Errorf(internal.Forced, "Failed to store the offset: %s.", err)
	}
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.printer.Info(internal.Verbose, "Closing Kafka consumer.")
		err := c.internalConsumer.Close()
		if err != nil {
			c.printer.Errorf(internal.Forced, "Failed to close the Kafka consumer: %s.", err)
		}
		c.printer.Info(internal.Verbose, "Closing Kafka connections.")
		err = c.internalClient.Close()
		if err != nil {
			c.printer.Errorf(internal.Forced, "Failed to close Kafka connections: %s.", err)
		} else {
			c.printer.Info(internal.VeryVerbose, "The Kafka client has been closed successfully.")
		}
		close(c.events)
	})
}

func (c *Consumer) consumeTopics(ctx context.Context, topicPartitionOffsets TopicPartitionOffset) {
	cn, cancel := context.WithCancel(ctx)
	defer cancel()
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
				case <-cn.Done():
					cancelled = true
					break
				default:
					c.wg.Add(1)
					go func() {
						err := c.consumePartition(cn, t, p, o)
						if err != nil {
							c.printer.Errorf(internal.Forced, "Failed to start consuming from %v offset of topic %s, partition %d: %s", getOffsetString(offset.Current), t, p, err)
							cancel()
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
	select {
	case <-ctx.Done():
		return nil
	default:
		pc, err := c.internalConsumer.ConsumePartition(topic, partition, offset.Current)
		if err != nil {
			return fmt.Errorf("failed to start consuming partition %d of topic %s: %w", partition, topic, err)
		}

		c.wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer func() {
				pc.AsyncClose()
				c.wg.Done()
			}()
			for {
				select {
				case <-ctx.Done():
					return
				case m, more := <-pc.Messages():
					if !more {
						return
					}
					c.events <- &Event{
						Topic:     m.Topic,
						Key:       m.Key,
						Value:     m.Value,
						Timestamp: m.Timestamp,
						Partition: m.Partition,
						Offset:    m.Offset,
					}
				}
			}
		}(pc)

		c.wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer c.wg.Done()
			for err := range pc.Errors() {
				c.printer.Errorf(internal.Forced, "Failed to consume message: %s.", err)
			}
		}(pc)

		return nil
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

	localOffsetManager := NewLocalOffsetManager(c.printer.Level())
	for topic, checkpoints := range topics {
		if !c.enableAutoTopicCreation {
			if _, ok := existing[topic]; !ok {
				return nil, fmt.Errorf("failed to find the topic %s on the server. You must create the topic manually or enable automatic topic creation both on the server and in trubka", topic)
			}
		}
		c.printer.Logf(internal.SuperVerbose, "Fetching partitions for topic %s.", topic)
		offsets := make(PartitionOffset)
		if checkpoints.mode == LocalOffsetMode {
			localOffsets, err := localOffsetManager.ReadLocalTopicOffsets(topic, c.environment)
			if err != nil {
				return nil, err
			}
			offsets = localOffsets
		}

		partitions, err := c.internalConsumer.Partitions(topic)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the partition offsets for topic %s: %w", topic, err)
		}

		for _, partition := range partitions {
			offset := sarama.OffsetNewest
			switch checkpoints.mode {
			case ExplicitOffsetMode:
				cp, found := checkpoints.Get(partition)
				if !found {
					// No need to start consuming the partition because it was not
					// explicitly asked by the user
					c.printer.Logf(internal.SuperVerbose, "Partition %d of topic %s has been ignored as per user request", partition, topic)
					continue
				}
				c.printer.Logf(internal.SuperVerbose, "Reading the most recent offset of partition %d for topic %s from the server", partition, topic)
				mostRecentOffset, err := c.internalClient.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					return nil, fmt.Errorf("failed to retrieve the current offset value for partition %d of topic %s: %w", partition, topic, err)
				}
				c.printer.Logf(internal.SuperVerbose,
					"The most recent offset for partition %d of topic %s is %v on the server. You asked for offset %v",
					partition,
					topic,
					getOffsetString(mostRecentOffset),
					cp.OffsetString())

				offset = int64(math.Min(float64(cp.offset), float64(mostRecentOffset)))
			case MillisecondsOffsetMode:
				cp := checkpoints.GetDefault()
				c.printer.Logf(internal.SuperVerbose,
					"Reading the most recent offset value for partition %d of topic %s at %s from the server",
					partition,
					topic,
					cp.OffsetString())
				offset, err = c.internalClient.GetOffset(topic, partition, cp.offset)
				if err != nil {
					return nil, fmt.Errorf("failed to retrieve the time-based offset for partition %d of topic %s: %w", partition, topic, err)
				}
				c.printer.Logf(internal.SuperVerbose,
					"The most recent available offset of partition %d of topic %s at %v is %v",
					partition,
					topic,
					internal.FormatTimeUTC(cp.at),
					getOffsetString(offset))
			case LocalOffsetMode:
				cp := checkpoints.GetDefault()
				offset = cp.offset
				c.printer.Logf(internal.SuperVerbose,
					"Checking the local offset store for partition %d of topic %s in %s environment",
					partition,
					topic,
					c.environment)
				if storedOffset, ok := offsets[partition]; ok {
					offset = storedOffset.Current
					c.printer.Logf(internal.SuperVerbose,
						"The local offset for partition %d of topic %s in %s environment is %v",
						partition,
						topic,
						c.environment,
						getOffsetString(offset))
				} else {
					c.printer.Logf(internal.SuperVerbose,
						"No local offset found for partition %d of topic %s",
						partition,
						topic)
				}
			default:
				cp := checkpoints.GetDefault()
				offset = cp.offset
			}
			c.printer.Logf(internal.SuperVerbose,
				"Consuming from offset %v of partition %d from topic %s.",
				getOffsetString(offset),
				partition,
				topic)
			offsets[partition] = Offset{Current: offset}
		}
		topicPartitionOffsets[topic] = offsets
	}
	return topicPartitionOffsets, nil
}

func getOffsetString(offset int64) interface{} {
	switch offset {
	case sarama.OffsetOldest:
		return "'oldest'"
	case sarama.OffsetNewest:
		return "'newest'"
	default:
		return offset
	}
}
