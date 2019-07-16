package kafka

import (
	"context"
	"path/filepath"
	"sync"

	"github.com/Shopify/sarama"
	"github.com/kirsle/configdir"
	"github.com/pkg/errors"

	"go.xitonix.io/trubka/internal"
)

type Consumer struct {
	brokers         []string
	config          *Options
	printer         internal.Printer
	client          sarama.Consumer
	topicPartitions map[string][]int32
	wg              sync.WaitGroup
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

	client, err := initClient(brokers, ops)
	if err != nil {
		return nil, errors.Wrap(err, "failed to initialise kafka client")
	}

	return &Consumer{
		config:          ops,
		brokers:         brokers,
		printer:         printer,
		client:          client,
		topicPartitions: make(map[string][]int32),
	}, nil
}

func (c *Consumer) Start(ctx context.Context, topics []string, cb Callback) error {
	if len(topics) == 0 {
		return errors.New("the topic list cannot be empty")
	}
	if cb == nil {
		return errors.New("consumer callback function cannot be nil")
	}

	err := c.fetchTopicPartitions(topics)
	if err != nil {
		return err
	}
	return c.consumeTopics(ctx, cb)
}

func (c *Consumer) consumeTopics(ctx context.Context, cb Callback) error {
	offset := sarama.OffsetNewest
	if c.config.Rewind {
		offset = sarama.OffsetOldest
	}

	cn, cancel := context.WithCancel(ctx)
	defer cancel()
	for topic, partitions := range c.topicPartitions {
		select {
		case <-ctx.Done():
			break
		case <-cn.Done():
			break
		default:
			for _, partition := range partitions {
				err := c.consumePartition(cn, cb, topic, partition, offset)
				if err != nil {
					c.printer.Writef(internal.Quiet, "Failed to start consuming from offset %d partition %d of topic %s: %s", offset, partition, topic, err)
					cancel()
					break
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
	c.printer.Writeln(internal.SuperVerbose, "Closing the offset store.")
	if c.config.OffsetStore != nil {
		return c.config.OffsetStore.Close()
	}
	return nil
}

func (c *Consumer) consumePartition(ctx context.Context, cb Callback, topic string, partition int32, offset int64) error {
	c.printer.Writef(internal.SuperVerbose, "Start consuming from %s topic, partition %d, offset %d.\n", topic, partition, offset)
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
				err := c.config.OffsetStore.Store(m.Topic, m.Partition, m.Offset)
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
	for _, topic := range topics {
		partitions, err := c.client.Partitions(topic)
		if err != nil {
			return errors.Wrapf(err, "failed to fetch the topicPartitions for topic %s", topic)
		}
		c.topicPartitions[topic] = partitions
	}
	return nil
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
