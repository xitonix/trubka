package kafka

import (
	"errors"
	"sync"
	"time"

	"github.com/Shopify/sarama"
	"github.com/araddon/dateparse"
)

var (
	deliberateErr = errors.New("asked by user")
)

const (
	_endOfStream = int64(100)
)

type clientMock struct {
	mux                                sync.Mutex
	counter                            int
	partitionConsumers                 map[string]map[int32]*partitionConsumerMock
	topics                             []string
	partitions                         []int32
	topicNotFound                      bool
	forceTopicsQueryFailure            bool
	forcePartitionsQueryFailure        bool
	forceOffsetQueryFailure            bool
	forcePartitionConsumerCloseFailure bool
	publishStartTime                   time.Time
	ready                              chan interface{}
	availableOffsets                   map[int32]map[int64]int64
	numberOfActivePartitions           int
}

func newClientMock(
	topics []string,
	numberOfPartitions int,
	numberOfActivePartitions int,
	topicNotFound bool,
	forceTopicListFailure bool,
	forcePartitionsQueryFailure bool,
	forceOffsetQueryFailure bool) *clientMock {
	available := make(map[int32]map[int64]int64)
	partitions := make([]int32, numberOfPartitions)
	for i := 0; i < numberOfPartitions; i++ {
		partitions[i] = int32(i)
		available[int32(i)] = map[int64]int64{
			sarama.OffsetNewest: _endOfStream,
			sarama.OffsetOldest: 0,
		}
	}
	if numberOfActivePartitions == 0 {
		numberOfActivePartitions = numberOfPartitions
	}
	cm := &clientMock{
		topics:                      topics,
		partitionConsumers:          make(map[string]map[int32]*partitionConsumerMock),
		partitions:                  partitions,
		topicNotFound:               topicNotFound,
		forceTopicsQueryFailure:     forceTopicListFailure,
		forcePartitionsQueryFailure: forcePartitionsQueryFailure,
		forceOffsetQueryFailure:     forceOffsetQueryFailure,
		ready:                       make(chan interface{}),
		availableOffsets:            available,
		numberOfActivePartitions:    numberOfActivePartitions,
	}

	for _, topic := range topics {
		cm.partitionConsumers[topic] = make(map[int32]*partitionConsumerMock)
		for _, partition := range partitions {
			cm.partitionConsumers[topic][partition] = newPartitionConsumerMock(
				topic,
				partition,
				sarama.OffsetNewest)
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
	if offset == sarama.OffsetNewest {
		offset = _endOfStream
	}
	if offset == sarama.OffsetOldest {
		offset = 0
	}
	c.partitionConsumers[topic][partition] = newPartitionConsumerMock(topic, partition, offset)
	c.counter++
	if c.counter == c.numberOfActivePartitions {
		close(c.ready)
	}

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

func (c *clientMock) GetOffset(_ string, partition int32, offset int64) (int64, error) {
	if c.forceOffsetQueryFailure {
		return unknownOffset, deliberateErr
	}
	return c.availableOffsets[partition][offset], nil
}

func (c *clientMock) Close() error {
	return nil
}

func (c *clientMock) receive(topic string, partition int32, at string) {
	t, _ := dateparse.ParseAny(at)
	c.partitionConsumers[topic][partition].receive(t)
}

func (c *clientMock) setAvailableOffset(partition int32, at interface{}, offset int64) {
	switch value := at.(type) {
	case int64:
		c.availableOffsets[partition][value] = offset
	case time.Time:
		c.availableOffsets[partition][value.UnixNano()/1000000] = offset
	case string:
		t, _ := dateparse.ParseAny(value)
		c.availableOffsets[partition][t.UnixNano()/1000000] = offset
	}
}
