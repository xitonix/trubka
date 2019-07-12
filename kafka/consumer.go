package kafka

import (
	"context"
	"sync"

	"github.com/Shopify/sarama"
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
	var err error
	for topic, partitions := range c.topicPartitions {
		if err != nil {
			break
		}
		for _, partition := range partitions {
			err = c.consumePartition(cn, cb, topic, partition, offset)
			if err != nil {
				cancel()
				break
			}
		}
	}
	c.wg.Wait()
	return err
}

func (c *Consumer) consumePartition(ctx context.Context, cb Callback, topic string, partition int32, offset int64) error {
	pc, err := c.client.ConsumePartition(topic, partition, offset)
	if err != nil {
		return errors.Wrapf(err, "failed to start consuming partition %d of topic %s", partition, topic)
	}

	go func(pc sarama.PartitionConsumer) {
		<-ctx.Done()
		err := pc.Close()
		if err != nil {
			c.printer.Writef(internal.Quiet, "failed to close the message: %s")
		}

	}(pc)

	c.wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		defer c.wg.Done()
		for m := range pc.Messages() {
			cb(m.Topic, m.Partition, m.Offset, m.Timestamp, m.Key, m.Value)
		}
	}(pc)

	c.wg.Add(1)
	go func(pc sarama.PartitionConsumer) {
		defer c.wg.Done()
		for err := range pc.Errors() {
			c.printer.Writef(internal.Quiet, "failed to consume message: %s", err)
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
