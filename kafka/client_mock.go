package kafka

import (
	"github.com/Shopify/sarama"
)

type clientMock struct {
	partitionConsumers map[string]map[int32]*partitionConsumerMock
	topics             []string
	partitions         []int32
}

func newClientMock(topics []string, numberOfPartitions int) *clientMock {
	partitions := make([]int32, numberOfPartitions)
	for i := 0; i < numberOfPartitions; i++ {
		partitions[i] = int32(i)
	}
	cm := &clientMock{
		topics:             topics,
		partitionConsumers: make(map[string]map[int32]*partitionConsumerMock),
		partitions:         partitions,
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
	return c.partitions, nil
}

func (c *clientMock) ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error) {
	if _, ok := c.partitionConsumers[topic][partition]; !ok {
		c.partitionConsumers[topic][partition] = newPartitionConsumerMock(topic, partition, offset)
	}
	return c.partitionConsumers[topic][partition], nil
}

func (c *clientMock) Topics() ([]string, error) {
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
