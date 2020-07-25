package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

// ConsumerWrapper wraps Sarama consumer and its underlying client.
type ConsumerWrapper struct {
	sarama.Consumer
	client sarama.Client
}

// NewConsumerWrapper creates a new instance of consumer wrapper.
func NewConsumerWrapper(brokers []string, options ...Option) (*ConsumerWrapper, error) {
	client, err := initClient(brokers, options...)
	if err != nil {
		return nil, err
	}
	consumer, err := sarama.NewConsumerFromClient(client)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise the Kafka consumer: %w", err)
	}

	return &ConsumerWrapper{
		Consumer: consumer,
		client:   client,
	}, nil
}

// GetOffset queries the cluster to get the most recent available offset at the
// given time (in milliseconds) on the topic/partition combination.
func (c *ConsumerWrapper) GetOffset(topic string, partitionID int32, time int64) (int64, error) {
	return c.client.GetOffset(topic, partitionID, time)
}
