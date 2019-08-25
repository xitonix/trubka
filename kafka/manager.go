package kafka

import (
	"context"
	"log"
	"regexp"
	"sort"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"
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

// GetTopics loads a list of the available topics from the server.
func (m *Manager) GetTopics(ctx context.Context, filter *regexp.Regexp, includeOffsets bool) (map[string]PartitionsOffsetPair, error) {
	topics, err := m.client.Topics()
	result := make(map[string]PartitionsOffsetPair)
	if err != nil {
		return nil, err
	}

	for _, topic := range topics {
		select {
		case <-ctx.Done():
			return result, nil
		default:
			if filter.Match([]byte(topic)) {
				result = append(result, topic)
			}
		}
	}
	sort.Strings(result)
	return result, nil
}

// GetBrokers returns the current set of active brokers as retrieved from cluster metadata.
func (m *Manager) GetBrokers(ctx context.Context, includeMetadata bool) ([]Broker, error) {
	brokers := m.client.Brokers()
	result := make([]Broker, 0)
	for _, broker := range brokers {
		select {
		case <-ctx.Done():
			return result, nil
		default:
			b := Broker{
				ID:      int(broker.ID()),
				Address: broker.Addr(),
			}
			if includeMetadata {
				m, err := m.getMetadata(broker)
				if err != nil {
					return nil, err
				}
				b.Meta = m
			}
			result = append(result, b)
		}
	}
	return result, nil
}

// Close closes the underlying Kafka connection.
func (m *Manager) Close() error {
	return m.client.Close()
}

func (m *Manager) getMetadata(broker *sarama.Broker) (*BrokerMetadata, error) {
	meta := &BrokerMetadata{
		Topics: make([]Topic, 0),
	}
	if err := broker.Open(m.client.Config()); err != nil {
		return nil, err
	}
	defer func() {
		_ = broker.Close()
	}()

	mt, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		return nil, err
	}
	meta.Version = int(mt.Version)
	for _, topic := range mt.Topics {
		if topic != nil {
			meta.Topics = append(meta.Topics, Topic{
				Name:               topic.Name,
				NumberOdPartitions: len(topic.Partitions),
			})
		}
	}
	sort.Sort(TopicsByName(meta.Topics))
	return meta, nil
}
