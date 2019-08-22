package kafka

import (
	"context"
	"log"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/commands/models"
)

// Manager a type to query Kafka metadata.
type Manager struct {
	config *Options
	client sarama.Client
}

// NewManager creates a new instance of Kafka manager
func NewManager(brokers []string, options ...Option) (*Manager, error) {
	if len(brokers) == 0 {
		return nil, errors.New("The brokers list cannot be empty")
	}
	ops := NewOptions()
	for _, option := range options {
		option(ops)
	}

	sarama.Logger = log.New(ops.logWriter, "KAFKA Client: ", log.LstdFlags)

	client, err := initClient(brokers, ops)
	if err != nil {
		return nil, err
	}

	return &Manager{
		config: ops,
		client: client,
	}, nil
}

// GetBrokers returns the current set of active brokers as retrieved from cluster metadata.
func (m *Manager) GetBrokers(ctx context.Context) ([]models.Broker, error) {
	brokers := m.client.Brokers()
	result := make([]models.Broker, 0)
	for _, broker := range brokers {
		select {
		case <-ctx.Done():
			return result, nil
		default:
			result = append(result, models.Broker{
				ID:      int(broker.ID()),
				Address: broker.Addr(),
				Rack:    broker.Rack(),
			})
		}
	}
	return result, nil
}

// Close closes the underlying Kafka connection.
func (m *Manager) Close() error {
	return m.client.Close()
}
