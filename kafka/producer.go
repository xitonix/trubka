package kafka

import (
	"github.com/Shopify/sarama"
)

type Producer struct {
	producer sarama.SyncProducer
}

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

func (p *Producer) Produce(topic string, key, value []byte) (int32, int64, error) {
	message := &sarama.ProducerMessage{
		Topic: topic,
		Key:   sarama.ByteEncoder(key),
		Value: sarama.ByteEncoder(value)}

	return p.producer.SendMessage(message)
}

func (p *Producer) Close() error {
	if p.producer != nil {
		return p.producer.Close()
	}
	return nil
}
