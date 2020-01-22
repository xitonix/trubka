package kafka

import (
	"context"
	"errors"
	"fmt"
	"regexp"

	"github.com/Shopify/sarama"

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
		Members: make(GroupMembers),
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
				md, err := fromGroupMemberDescription(description)
				if err != nil {
					return nil, err
				}
				cgd.Members[name] = md
			}
		}

	}

	return cgd, nil
}

func (m *Manager) DescribeBroker(ctx context.Context, address string, includeLogs bool, topicFilter *regexp.Regexp) (*BrokerMeta, error) {
	meta := &BrokerMeta{
		Logs: make([]*LogFile, 0),
	}
	select {
	case <-ctx.Done():
		return meta, nil
	default:
		broker, ok := m.serversByAddress[address]
		if !ok {
			return nil, fmt.Errorf("broker %s not found", address)
		}
		m.Logf(internal.Verbose, "Connecting to %s", address)
		err := broker.Open(m.client.Config())
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", address, err)
		}
		defer func() {
			_ = broker.Close()
		}()
		m.Logf(internal.Verbose, "Fetching consumer groups from %s", address)
		response, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return nil, err
		}
		meta.ConsumerGroups = make([]string, len(response.Groups))
		var i int
		for group := range response.Groups {
			m.Logf(internal.VeryVerbose, "Consumer group %s has been retrieved from %s", group, address)
			meta.ConsumerGroups[i] = group
			i++
		}

		if includeLogs {
			m.Logf(internal.VeryVerbose, "Retrieving broker log details from %s", address)
			logs, err := broker.DescribeLogDirs(&sarama.DescribeLogDirsRequest{})
			if err != nil {
				return nil, err
			}
			for _, l := range logs.LogDirs {
				lf := newLogFile(l.Path)
				for _, topicLog := range l.Topics {
					for _, partitionLog := range topicLog.Partitions {
						if topicFilter != nil && !topicFilter.Match([]byte(topicLog.Topic)) {
							m.Logf(internal.SuperVerbose, "The provided topic filter (%s) does not match with %s topic", topicFilter.String(), topicLog.Topic)
							continue
						}
						m.Logf(internal.Chatty, "Log file entry retrieved for partition %d of topic %s", partitionLog.PartitionID, topicLog.Topic)
						lf.set(topicLog.Topic, partitionLog.Size, partitionLog.IsTemporary)
					}
				}
				meta.Logs = append(meta.Logs, lf)
			}
		}
	}
	return meta, nil
}

func (m *Manager) GetGroupOffsets(ctx context.Context, group string, topicFilter *regexp.Regexp) (TopicPartitionOffset, error) {
	result := make(TopicPartitionOffset)
	select {
	case <-ctx.Done():
		return result, nil
	default:
		m.Log(internal.Verbose, "Retrieving consumer group details")
		groupDescriptions, err := m.admin.DescribeConsumerGroups([]string{group})
		if err != nil {
			return nil, err
		}

		if len(groupDescriptions) != 1 {
			return nil, errors.New("failed to retrieve consumer group details from the server")
		}
		topicPartitions := make(map[string][]int32)
		for _, member := range groupDescriptions[0].Members {
			select {
			case <-ctx.Done():
				return result, nil
			default:
				m.Logf(internal.VeryVerbose, "Retrieving the topic assignments for %s", member.ClientId)
				assignments, err := member.GetMemberAssignment()
				if err != nil {
					return nil, fmt.Errorf("failed to retrieve the topic/partition assignments: %w", err)
				}

				for topic, partitions := range assignments.Topics {
					if topicFilter != nil && !topicFilter.Match([]byte(topic)) {
						continue
					}
					if _, ok := topicPartitions[topic]; !ok {
						topicPartitions[topic] = make([]int32, 0)
					}
					for _, partition := range partitions {
						topicPartitions[topic] = append(topicPartitions[topic], partition)
					}
				}

				for topic, partitions := range topicPartitions {
					result[topic] = make(PartitionOffset)
					for _, partition := range partitions {
						result[topic][partition] = Offset{}
					}
				}
			}
		}

		m.Logf(internal.VeryVerbose, "Retrieving the offsets for %s consumer group", group)
		cgOffsets, err := m.admin.ListConsumerGroupOffsets(group, topicPartitions)
		if err != nil {
			return nil, fmt.Errorf("failed to retrieve the consumer group offsets: %w", err)
		}

		for topic, blocks := range cgOffsets.Blocks {
			for partition, group := range blocks {
				select {
				case <-ctx.Done():
					return result, nil
				default:
					if group.Offset < 0 {
						continue
					}
					m.Logf(internal.SuperVerbose, "Retrieving the latest offset of partition %d of %s topic from the server", partition, topic)
					latestTopicOffset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						return nil, err
					}
					result[topic][partition] = Offset{
						Current: group.Offset,
						Latest:  latestTopicOffset,
					}
				}
			}
		}
	}
	return result, nil
}

func (m *Manager) GetTopicOffsets(ctx context.Context, topic string, currentPartitionOffsets PartitionOffset) (PartitionOffset, error) {
	result := make(PartitionOffset)
	select {
	case <-ctx.Done():
		return result, nil
	default:
		m.Logf(internal.Verbose, "Retrieving %s topic offsets from the server", topic)
		for partition, offset := range currentPartitionOffsets {
			select {
			case <-ctx.Done():
				return result, nil
			default:
				latestTopicOffset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					return nil, err
				}
				result[partition] = Offset{
					Latest:  latestTopicOffset,
					Current: offset.Current,
				}
			}
		}
	}
	return result, nil
}
