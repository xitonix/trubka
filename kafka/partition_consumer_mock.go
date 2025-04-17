package kafka

import (
	"sync"
	"time"

	"github.com/Shopify/sarama"
)

type partitionConsumerMock struct {
	mux       sync.Mutex
	closed    bool
	messages  chan *sarama.ConsumerMessage
	errors    chan *sarama.ConsumerError
	offset    int64
	partition int32
	topic     string
}

func newPartitionConsumerMock(topic string, partition int32, offset int64) *partitionConsumerMock {
	return &partitionConsumerMock{
		messages:  make(chan *sarama.ConsumerMessage, 100),
		errors:    make(chan *sarama.ConsumerError, 100),
		topic:     topic,
		partition: partition,
		offset:    offset,
	}
}

func (p *partitionConsumerMock) AsyncClose() {
	_ = p.Close()
}

func (p *partitionConsumerMock) Close() error {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.closed {
		return nil
	}
	p.closed = true
	close(p.messages)
	close(p.errors)
	return nil
}

func (p *partitionConsumerMock) receive(at time.Time) {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.closed {
		return
	}

	p.messages <- &sarama.ConsumerMessage{
		Timestamp: at,
		Key:       []byte("key"),
		Value:     []byte("value"),
		Topic:     p.topic,
		Partition: p.partition,
		Offset:    p.offset,
	}
	p.offset++
}

func (p *partitionConsumerMock) Messages() <-chan *sarama.ConsumerMessage {
	return p.messages
}

func (p *partitionConsumerMock) Errors() <-chan *sarama.ConsumerError {
	return p.errors
}

func (p *partitionConsumerMock) HighWaterMarkOffset() int64 {
	return p.offset + 1
}
