package kafka

import (
	"errors"
	"sync"

	"github.com/Shopify/sarama"
)

var (
	deliberateErr = errors.New("asked by user")
)

type clientMock struct {
	mux                         sync.Mutex
	partitionConsumers          map[string]map[int32]*partitionConsumerMock
	topics                      []string
	partitions                  []int32
	topicNotFound               bool
	forceTopicsQueryFailure     bool
	forcePartitionsQueryFailure bool
}

func newClientMock(
	topics []string,
	numberOfPartitions int,
	topicNotFound bool,
	forceTopicListFailure bool,
	forcePartitionsQueryFailure bool) *clientMock {

	partitions := make([]int32, numberOfPartitions)
	for i := 0; i < numberOfPartitions; i++ {
		partitions[i] = int32(i)
	}
	cm := &clientMock{
		topics:                      topics,
		partitionConsumers:          make(map[string]map[int32]*partitionConsumerMock),
		partitions:                  partitions,
		topicNotFound:               topicNotFound,
		forceTopicsQueryFailure:     forceTopicListFailure,
		forcePartitionsQueryFailure: forcePartitionsQueryFailure,
	}

	for _, topic := range topics {
		cm.partitionConsumers[topic] = make(map[int32]*partitionConsumerMock)
		for _, partition := range partitions {
			cm.partitionConsumers[topic][partition] = nil
		}
	}
	return cm
}

func (c *clientMock) Partitions(_ string) ([]int32, error) {
	if c.forcePartitionsQueryFailure {
		return nil, deliberateErr
	}
	return c.partitions, nil
}

func (c *clientMock) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	c.mux.Lock()
	defer c.mux.Unlock()
	c.partitionConsumers[topic][partition] = newPartitionConsumerMock(topic, partition, offset)
	return c.partitionConsumers[topic][partition], nil
}

func (c *clientMock) Topics() ([]string, error) {
	if c.forceTopicsQueryFailure {
		return nil, deliberateErr
	}
	if c.topicNotFound {
		// Simulating the behaviour when the topic was not found on the server
		return []string{}, nil
	}
	return c.topics, nil
}

func (c *clientMock) GetOffset(topic string, partition int32, _ int64) (int64, error) {
	return c.partitionConsumers[topic][partition].getLatest(), nil
}

func (c *clientMock) Close() error {
	return nil
}

func (c *clientMock) setLatestOffset(topic string, partition int32, offset int64) {
	c.partitionConsumers[topic][partition].setLatest(offset)
}

func (c *clientMock) receive(topic string, partition int32) {
	c.partitionConsumers[topic][partition].receive()
}
