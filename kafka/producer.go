package kafka

import (
	"github.com/Shopify/sarama"
)

// Producer represents a wrapper around Sarama producer.
type Producer struct {
	producer sarama.SyncProducer
}

// NewProducer creates a new instance of Kafka producer.
func NewProducer(brokers []string, options ...Option) (*Producer, error) {
	client, err := initClient(brokers, options...)
	if err != nil {
		return nil, err
	}

	producer, err := sarama.NewSyncProducerFromClient(client)
	if err != nil {
		return nil, err
	}

	return &Producer{
		producer: producer,
	}, nil
}

// Produce publishes a new message to the specified Kafka topic.
func (p *Producer) Produce(topic string, key, value []byte) (int32, int64, error) {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value)}
	return p.producer.SendMessage(message)
}

// Close closes the producer.
func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
