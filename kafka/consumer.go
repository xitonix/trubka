package kafka

import (
	"context"
	"log"
	"math"
	"os"
	"regexp"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kirsle/configdir"
	"github.com/pkg/errors"
	"github.com/rcrowley/go-metrics"

	"github.com/xitonix/trubka/internal"
)

// Consumer represents a new Kafka cluster consumer.
type Consumer struct {
	brokers                 []string
	config                  *Options
	printer                 internal.Printer
	internalConsumer        sarama.Consumer
	internalClient          sarama.Client
	wg                      sync.WaitGroup
	remoteTopics            []string
	enableAutoTopicCreation bool
	environment             string
	events                  chan *Event
	closeOnce               sync.Once
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

	if printer.Level() == internal.Chatty {
		sarama.Logger = log.New(os.Stdout, "KAFKA Client: ", log.LstdFlags)
	}

	client, consumer, err := initClient(brokers, ops)
	if err != nil {
		return nil, err
	}

	return &Consumer{
		config:                  ops,
		brokers:                 brokers,
		printer:                 printer,
		internalConsumer:        consumer,
		internalClient:          client,
		enableAutoTopicCreation: enableAutoTopicCreation,
		environment:             environment,
		events:                  make(chan *Event, 128),
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

	topics, err := c.internalConsumer.Topics()
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
func (c *Consumer) Start(ctx context.Context, topics map[string]*Checkpoint) error {
	if len(topics) == 0 {
		return errors.New("the topic list cannot be empty")
	}

	if c.config.OffsetStore == nil {
		store, err := c.initialiseLocalOffsetStore()
		if err != nil {
			return err
		}
		c.config.OffsetStore = store
	}

	c.printer.Infof(internal.VeryVerbose, "Starting Kafka consumers.")
	topicPartitionOffsets, err := c.fetchTopicPartitions(topics)
	if err != nil {
		return err
	}

	if store, ok := c.config.OffsetStore.(*localOffsetStore); ok {
		defer store.close()
		store.start(topicPartitionOffsets)
	}
	c.consumeTopics(ctx, topicPartitionOffsets)
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
		for err := range ls.errors() {
			c.printer.Errorf(internal.Forced, "Err: %s", err)
		}
	}()

	return ls, nil
}

// StoreOffset stores the offset of the successfully processed message into the offset store.
func (c *Consumer) StoreOffset(event *Event) {
	if c.config.OffsetStore != nil {
		err := c.config.OffsetStore.Store(event.Topic, event.Partition, event.Offset+1)
		if err != nil {
			c.printer.Errorf(internal.Forced, "Failed to store the offset: %s.", err)
		}
	}
}

// Close closes the Kafka consumer.
func (c *Consumer) Close() {
	c.closeOnce.Do(func() {
		c.printer.Info(internal.Verbose, "Closing Kafka consumer.")
		err := c.internalConsumer.Close()
		if err != nil {
			c.printer.Errorf(internal.Forced, "Failed to close Kafka client: %s.", err)
		} else {
			c.printer.Info(internal.VeryVerbose, "The Kafka client has been closed successfully.")
		}
		close(c.events)
	})
}

func (c *Consumer) consumeTopics(ctx context.Context, topicPartitionOffsets map[string]PartitionOffsets) {
	cn, cancel := context.WithCancel(ctx)
	defer cancel()
	var cancelled bool
	for topic, partitionOffsets := range topicPartitionOffsets {
		if cancelled {
			break
		}
		select {
		case <-ctx.Done():
			cancelled = true
			break
		default:
			for partition, offset := range partitionOffsets {
				if cancelled {
					break
				}
				select {
				case <-cn.Done():
					cancelled = true
					break
				default:
					err := c.consumePartition(cn, topic, partition, offset)
					if err != nil {
						c.printer.Errorf(internal.Forced, "Failed to start consuming from %s offset of topic %s, partition %d: %s", getOffsetString(offset), topic, partition, err)
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

func (c *Consumer) consumePartition(ctx context.Context, topic string, partition int32, offset int64) error {
	select {
	case <-ctx.Done():
		return nil
	default:
		c.printer.Logf(internal.VeryVerbose, "Start consuming from partition %d of topic %s (offset: %v).", partition, topic, getOffsetString(offset))
		pc, err := c.internalConsumer.ConsumePartition(topic, partition, offset)
		if err != nil {
			return errors.Wrapf(err, "Failed to start consuming partition %d of topic %s", partition, topic)
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

func (c *Consumer) fetchTopicPartitions(topics map[string]*Checkpoint) (map[string]PartitionOffsets, error) {
	existing := make(map[string]interface{})
	if !c.enableAutoTopicCreation {
		// We need to check if the requested topic(s) exist on the server
		// That's why we need to get the list of the existing topics from the brokers.
		remote, err := c.GetTopics("")
		if err != nil {
			return nil, errors.Wrapf(err, "Failed to fetch the topic list from the broker(s)")
		}
		for _, t := range remote {
			existing[t] = nil
		}
	}

	topicPartitionOffsets := make(map[string]PartitionOffsets)

	for topic, cp := range topics {
		if !c.enableAutoTopicCreation {
			if _, ok := existing[topic]; !ok {
				return nil, errors.Errorf("failed to find the topic %s on the server. You must create the topic manually or enable automatic topic creation both on the server and in trubka", topic)
			}
		}
		c.printer.Logf(internal.SuperVerbose, "Fetching partitions for topic %s.", topic)
		var err error
		offsets := make(PartitionOffsets)
		if c.config.OffsetStore != nil {
			offsets, err = c.config.OffsetStore.Query(topic)
			if err != nil {
				return nil, err
			}
		}

		partitions, err := c.internalConsumer.Partitions(topic)
		if err != nil {
			return nil, errors.Wrapf(err, "failed to fetch the partition offsets for topic %s", topic)
		}
		for _, partition := range partitions {
			offset := sarama.OffsetNewest
			switch cp.mode {
			case ExplicitOffsetMode:
				c.printer.Logf(internal.SuperVerbose, "Reading the most recent offset of partition %d for topic %s from the server.", partition, topic)
				currentOffset, err := c.internalClient.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to retrieve the current offset value for partition %d of topic %s", partition, topic)
				}
				offset = int64(math.Min(float64(cp.offset), float64(currentOffset)))
				c.printer.Logf(internal.SuperVerbose,
					"The most recent offset for partition %d of topic %s: %d -> %d.",
					partition,
					topic,
					cp.offset,
					offset)
			case MillisecondsOffsetMode:
				c.printer.Logf(internal.SuperVerbose,
					"Reading the most recent offset value for partition %d of topic %s at %s from the server.",
					partition,
					topic,
					cp.OffsetString())
				offset, err = c.internalClient.GetOffset(topic, partition, cp.offset)
				if err != nil {
					return nil, errors.Wrapf(err, "failed to retrieve the time-based offset for partition %d of topic %s", partition, topic)
				}
				c.printer.Logf(internal.SuperVerbose,
					"Time-based offset for partition %d of topic %s at %v = %v.",
					partition,
					topic,
					internal.FormatTimeUTC(cp.at),
					getOffsetString(offset))
			default:
				if cp.offset == sarama.OffsetOldest {
					offset = cp.offset
					// We are in rewind mode. No need to read the locally stored offset value.
					break
				}
				if storedOffset, ok := offsets[partition]; ok {
					offset = storedOffset
				}
			}
			c.printer.Logf(internal.SuperVerbose,
				"Setting the offset of partition %d to %v for topic %s.",
				partition,
				getOffsetString(offset),
				topic)
			offsets[partition] = offset
		}
		topicPartitionOffsets[topic] = offsets
	}
	return topicPartitionOffsets, nil
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

func initClient(brokers []string, ops *Options) (sarama.Client, sarama.Consumer, error) {
	version, err := sarama.ParseKafkaVersion(ops.ClusterVersion)
	if err != nil {
		return nil, nil, err
	}
	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.ClientID = "Trubka"
	metrics.UseNilMetrics = true
	if ops.sasl != nil {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = ops.sasl.mechanism
		config.Net.SASL.User = ops.sasl.username
		config.Net.SASL.Password = ops.sasl.password
		config.Net.SASL.SCRAMClientGeneratorFunc = ops.sasl.client
	}

	if ops.TLS != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = ops.TLS
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialise the Kafka client")
	}

	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to initialise the Kafka consumer")
	}

	return client, consumer, nil
}
