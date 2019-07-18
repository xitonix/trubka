package kafka

import (
	"context"
	"path/filepath"
	"strconv"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kirsle/configdir"
	"github.com/pkg/errors"

	"go.xitonix.io/trubka/internal"
)

type Consumer struct {
	brokers               []string
	config                *Options
	printer               internal.Printer
	client                sarama.Consumer
	topicPartitionOffsets map[string]map[int32]int64
	wg                    sync.WaitGroup
}

func NewConsumer(brokers []string, printer internal.Printer, options ...Option) (*Consumer, error) {
	ops := NewOptions()
	for _, option := range options {
		option(ops)
	}

	if ops.OffsetStore == nil {
		configPath := configdir.LocalConfig("trubka")
		err := configdir.MakePath(configPath)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create the application cache folder")
		}
		ops.OffsetStore, err = newLocalOffsetStore(printer, filepath.Join(configPath, "offsets.bin"))
		if err != nil {
			return nil, err
		}
	}

	printer.Writef(internal.Verbose, "Initialising the Kafka client.")
	client, err := initClient(brokers, ops)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialise kafka client")
	}

	return &Consumer{
		config:                ops,
		brokers:               brokers,
		printer:               printer,
		client:                client,
		topicPartitionOffsets: make(map[string]map[int32]int64),
	}, nil
}

func (c *Consumer) Start(ctx context.Context, topics []string, cb Callback) error {
	if len(topics) == 0 {
		return errors.New("the topic list cannot be empty")
	}
	if cb == nil {
		return errors.New("consumer callback function cannot be nil")
	}

	c.printer.Writef(internal.Verbose, "Starting Kafka consumers.")

	err := c.fetchTopicPartitions(topics)
	if err != nil {
		return err
	}
	return c.consumeTopics(ctx, cb)
}

func (c *Consumer) consumeTopics(ctx context.Context, cb Callback) error {
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
						c.printer.Writef(internal.Quiet, "Failed to start consuming from %s offset of topic %s, partition %d: %s", getOffset(offset), topic, partition, err)
						cancel()
						cancelled = true
					}
				}
			}
		}
	}
	c.wg.Wait()
	c.printer.Writeln(internal.SuperVerbose, "Closing Kafka consumer.")
	err := c.client.Close()
	if err != nil {
		c.printer.Writef(internal.Quiet, "Failed to close Kafka client: %s.\n", err)
	} else {
		c.printer.Writeln(internal.Verbose, "The Kafka client has been closed successfully.")
	}

	if c.config.OffsetStore != nil {
		c.printer.Writeln(internal.SuperVerbose, "Closing the offset store.")
		return c.config.OffsetStore.Close()
	}
	return nil
}

func (c *Consumer) consumePartition(ctx context.Context, cb Callback, topic string, partition int32, offset int64) error {
	c.printer.Writef(internal.Verbose, "Start consuming from partition %d of topic %s (offset: %s).\n", partition, topic, getOffset(offset))
	pc, err := c.client.ConsumePartition(topic, partition, offset)
	if err != nil {
		return errors.Wrapf(err, "failed to start consuming partition %d of topic %s\n", partition, topic)
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
					c.printer.Writef(internal.Quiet, "Failed to store the offset for topic %s, partition %d: %s\n.", m.Topic, m.Partition, err)
				}
			}
		}
	}(pc)

	c.wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		defer c.wg.Done()
		for err := range pc.Errors() {
			c.printer.Writef(internal.Quiet, "Failed to consume message: %s\n.", err)
		}
	}(pc)

	return nil
}

func (c *Consumer) fetchTopicPartitions(topics []string) error {
	offset := sarama.OffsetNewest
	if c.config.Rewind {
		offset = sarama.OffsetOldest
	}
	for _, topic := range topics {
		c.printer.Writef(internal.SuperVerbose, "Fetching partitions for topic %s.\n", topic)
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
				c.printer.Writef(internal.SuperVerbose,
					"Setting the offset of partition %d to the stored value %d for topic %s.\n",
					partition,
					storedOffset,
					topic)
				offsets[partition] = storedOffset
			} else {
				c.printer.Writef(internal.SuperVerbose,
					"Setting the offset of partition %d to the %s value for topic %s.\n",
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
