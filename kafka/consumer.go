package kafka

import (
	"context"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kirsle/configdir"
	"github.com/pkg/errors"

	"go.xitonix.io/trubka/internal"
)

// Consumer represents a new Kafka cluster consumer.
type Consumer struct {
	brokers                 []string
	config                  *Options
	printer                 internal.Printer
	client                  sarama.Consumer
	topicPartitionOffsets   map[string]map[int32]int64
	wg                      sync.WaitGroup
	remoteTopics            []string
	enableAutoTopicCreation bool
	environment             string

	mux      sync.Mutex
	isClosed bool
}

// NewConsumer creates a new instance of Kafka cluster consumer.
func NewConsumer(brokers []string, printer internal.Printer, environment string, enableAutoTopicCreation bool, options ...Option) (*Consumer, error) {
	if len(brokers) == 0 {
		return nil, errors.New("The brokers list cannot be empty")
	}
	ops := NewOptions()
	for _, option := range options {
		option(ops)
	}

	client, err := initClient(brokers, ops)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to initialise kafka client")
	}

	return &Consumer{
		config:                  ops,
		brokers:                 brokers,
		printer:                 printer,
		client:                  client,
		topicPartitionOffsets:   make(map[string]map[int32]int64),
		enableAutoTopicCreation: enableAutoTopicCreation,
		environment:             environment,
	}, nil
}

// GetTopics fetches the topics from the server.
func (c *Consumer) GetTopics(filter string) ([]string, error) {
	if c.remoteTopics != nil {
		return c.remoteTopics, nil
	}
	var search *regexp.Regexp
	if !internal.IsEmpty(filter) {
		s, err := regexp.Compile(filter)
		if err != nil {
			return nil, errors.Wrap(err, "Invalid topic filter regular expression")
		}
		search = s
	}
	c.printer.Log(internal.SuperVerbose, "Fetching the topic list from the server.")
	topics, err := c.client.Topics()
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to fetch the topic list from the server")
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

// Start starts consuming from the specified topics and executes the callback function on each message.
//
// This is a blocking call which will be terminated on cancellation of the context parameter.
// The method returns error if the topic list is empty or the callback function is nil.
func (c *Consumer) Start(ctx context.Context, topics []string, cb Callback) error {
	if len(topics) == 0 {
		return errors.New("the topic list cannot be empty")
	}
	if cb == nil {
		return errors.New("consumer callback function cannot be nil")
	}

	if c.config.OffsetStore == nil {
		store, err := c.initialiseLocalOffsetStore()
		if err != nil {
			return err
		}
		c.config.OffsetStore = store
		defer store.Close()
	}

	c.printer.Logf(internal.VeryVerbose, "Starting Kafka consumers.")

	err := c.fetchTopicPartitions(topics)
	if err != nil {
		return err
	}
	c.consumeTopics(ctx, cb)
	return nil
}

func (c *Consumer) initialiseLocalOffsetStore() (*localOffsetStore, error) {
	var environment string
	if !internal.IsEmpty(c.environment) {
		environment = strings.ToLower(strings.TrimSpace(c.environment))
	}
	configPath := configdir.LocalConfig("trubka", environment)
	err := configdir.MakePath(configPath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create the application cache folder")
	}

	ls, err := newLocalOffsetStore(c.printer, configPath)
	if err != nil {
		return nil, err
	}

	go func() {
		for err := range ls.Errors() {
			c.printer.Logf(internal.Forced, "Err: %s", err)
		}
	}()

	ls.Start()

	return ls, nil
}

func (c *Consumer) consumeTopics(ctx context.Context, cb Callback) {
	cn, cancel := context.WithCancel(ctx)
	defer cancel()
	for topic, partitionOffsets := range c.topicPartitionOffsets {
		select {
		case <-ctx.Done():
			break
		case <-cn.Done():
			break
		default:
			var cancelled bool
			for partition, offset := range partitionOffsets {
				if cancelled {
					break
				}
				select {
				case <-ctx.Done():
					cancelled = true
					break
				default:
					err := c.consumePartition(cn, cb, topic, partition, offset)
					if err != nil {
						c.printer.Logf(internal.Forced, "Failed to start consuming from %s offset of topic %s, partition %d: %s", getOffset(offset), topic, partition, err)
						cancel()
						cancelled = true
					}
				}
			}
		}
	}
	c.wg.Wait()
	c.Close()
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() {
	c.mux.Lock()
	defer c.mux.Unlock()
	if c.isClosed || c.client == nil {
		return
	}
	c.printer.Log(internal.Verbose, "Closing Kafka consumer.")
	c.isClosed = true
	err := c.client.Close()
	if err != nil {
		c.printer.Logf(internal.Forced, "Failed to close Kafka client: %s.", err)
	} else {
		c.printer.Log(internal.VeryVerbose, "The Kafka client has been closed successfully.")
	}
}

func (c *Consumer) consumePartition(ctx context.Context, cb Callback, topic string, partition int32, offset int64) error {
	c.printer.Logf(internal.VeryVerbose, "Start consuming from partition %d of topic %s (offset: %s).", partition, topic, getOffset(offset))
	pc, err := c.client.ConsumePartition(topic, partition, offset)
	if err != nil {
		return errors.Wrapf(err, "Failed to start consuming partition %d of topic %s", partition, topic)
	}

	go func(pc sarama.PartitionConsumer) {
		<-ctx.Done()
		pc.AsyncClose()
	}(pc)

	c.wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		defer c.wg.Done()
		for m := range pc.Messages() {
			err := cb(m.Topic, m.Partition, m.Offset, m.Timestamp, m.Key, m.Value)
			if err == nil && c.config.OffsetStore != nil {
				err := c.config.OffsetStore.Store(m.Topic, m.Partition, m.Offset+1)
				if err != nil {
					c.printer.Logf(internal.Forced, "Failed to store the offset: %s.", err)
				}
			}
		}
	}(pc)

	c.wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		defer c.wg.Done()
		for err := range pc.Errors() {
			c.printer.Logf(internal.Forced, "Failed to consume message: %s.", err)
		}
	}(pc)

	return nil
}

func (c *Consumer) fetchTopicPartitions(topics []string) error {
	offset := sarama.OffsetNewest
	if c.config.Rewind {
		offset = sarama.OffsetOldest
	}

	existing := make(map[string]interface{})
	if !c.enableAutoTopicCreation {
		remote, err := c.GetTopics("")
		if err != nil {
			return errors.Wrapf(err, "Failed to fetch the topic list from the broker(s)")
		}
		for _, t := range remote {
			c.printer.Logf(internal.SuperVerbose, "Remote topic: %s", t)
			existing[t] = nil
		}
	}

	for _, topic := range topics {
		if !c.enableAutoTopicCreation {
			if _, ok := existing[topic]; !ok {
				return errors.Errorf("failed to find the topic %s on the server. You must create the topic manually or enable automatic topic creation both on the server and in trubka", topic)
			}
		}
		c.printer.Logf(internal.SuperVerbose, "Fetching partitions for topic %s.", topic)
		var err error
		offsets := make(map[int32]int64)
		if c.config.OffsetStore != nil && !c.config.ResetOffsets {
			offsets, err = c.config.OffsetStore.Query(topic)
			if err != nil {
				return err
			}
		}

		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return errors.Wrapf(err, "failed to fetch the partition offsets for topic %s", topic)
		}
		for _, partition := range partitions {
			if c.config.ResetOffsets {
				offsets[partition] = offset
				continue
			}
			if storedOffset, ok := offsets[partition]; ok {
				c.printer.Logf(internal.SuperVerbose,
					"Setting the offset of partition %d to the stored value %d for topic %s.",
					partition,
					storedOffset,
					topic)
				offsets[partition] = storedOffset
			} else {
				c.printer.Logf(internal.SuperVerbose,
					"Setting the offset of partition %d to the %s value for topic %s.",
					partition,
					getOffset(offset),
					topic)
				offsets[partition] = offset
			}
		}
		c.topicPartitionOffsets[topic] = offsets
	}
	return nil
}

func getOffset(offset int64) string {
	switch offset {
	case sarama.OffsetOldest:
		return "oldest"
	case sarama.OffsetNewest:
		return "newest"
	default:
		return strconv.FormatInt(offset, 10)
	}
}

func initClient(brokers []string, ops *Options) (sarama.Consumer, error) {
	version, err := sarama.ParseKafkaVersion(ops.ClusterVersion)
	if err != nil {
		return nil, err
	}
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true

	client, err := sarama.NewConsumer(brokers, config)
	if err != nil {
		return nil, err
	}
	return client, nil
}
