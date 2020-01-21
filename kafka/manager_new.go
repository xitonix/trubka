package kafka

import (
	"context"
	"fmt"
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
				ID:      broker.ID(),
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

func (m *Manager) DescribeGroup(ctx context.Context, group string, includeMembers bool) (*ConsumerGroupDetails, error) {
	m.Logf(internal.Verbose, "Retrieving %s consumer group details from the server", group)
	details, err := m.admin.DescribeConsumerGroups([]string{group})
	if err != nil {
		return nil, err
	}

	cgd := &ConsumerGroupDetails{
		Members: make(map[string]*GroupMemberDetails),
	}
	select {
	case <-ctx.Done():
		return cgd, nil
	default:
		if len(details) == 0 {
			return nil, fmt.Errorf("failed to retrieve the consumer group details of %s", group)
		}
		d := details[0]
		cgd.State = d.State
		cgd.Protocol = d.Protocol
		cgd.ProtocolType = d.ProtocolType
		coordinator, err := m.client.Coordinator(group)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the group coordinator details: %w", err)
		}
		cgd.Coordinator = Broker{
			Address: coordinator.Addr(),
			ID:      coordinator.ID(),
		}
		if includeMembers {
			for name, description := range d.Members {
				md, err := fromSaramaGroupMemberDescription(description)
				if err != nil {
					return nil, err
				}
				cgd.Members[name] = md
			}
		}

	}

	return cgd, nil
}
