package kafka

import (
	"context"
	"regexp"

	"github.com/xitonix/trubka/internal"
)

func (m *Manager) GetBrokers(ctx context.Context) ([]Broker, error) {
	m.Log(internal.Verbose, "Retrieving broker list from the server")
	result := make([]Broker, 0)
	for _, broker := range m.servers {
		select {
		case <-ctx.Done():
			return result, nil
		default:
			b := Broker{
				ID:      int(broker.ID()),
				Address: broker.Addr(),
			}
			result = append(result, b)
		}
	}
	return result, nil
}

func (m *Manager) GetTopics(ctx context.Context, filter *regexp.Regexp) ([]Topic, error) {
	m.Log(internal.Verbose, "Retrieving topic list from the server")
	topics, err := m.admin.ListTopics()
	if err != nil {
		return nil, err
	}

	result := make([]Topic, 0)
	for topic, details := range topics {
		m.Logf(internal.SuperVerbose, "Topic %s has been found on the server", topic)
		select {
		case <-ctx.Done():
			return result, nil
		default:
			if filter != nil && !filter.Match([]byte(topic)) {
				m.Logf(internal.SuperVerbose, "Filtering out %s topic", topic)
				continue
			}
			result = append(result, Topic{
				Name:               topic,
				NumberOfPartitions: details.NumPartitions,
				ReplicationFactor:  details.ReplicationFactor,
			})
		}
	}
	return result, nil
}

func (m *Manager) GetGroups(ctx context.Context, filter *regexp.Regexp) ([]string, error) {
	m.Log(internal.Verbose, "Retrieving consumer groups from the server")
	groups, err := m.admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}

	result := make([]string, 0)
	for group := range groups {
		m.Logf(internal.SuperVerbose, "Consumer group %s has been found on the server", group)
		select {
		case <-ctx.Done():
			return result, nil
		default:
			if filter != nil && !filter.Match([]byte(group)) {
				m.Logf(internal.SuperVerbose, "Filtering out %s consumer group", group)
				continue
			}
			result = append(result, group)
		}
	}
	return result, nil
}
