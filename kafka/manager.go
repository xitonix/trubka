package kafka

import (
	"context"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"

	"github.com/Shopify/sarama"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
)

// Manager a type to query Kafka metadata.
type Manager struct {
	config       *Options
	client       sarama.Client
	admin        sarama.ClusterAdmin
	localOffsets *LocalOffsetManager
	servers      []*sarama.Broker
	*internal.Logger
}

// NewManager creates a new instance of Kafka manager
func NewManager(brokers []string, verbosity internal.VerbosityLevel, options ...Option) (*Manager, error) {
	if len(brokers) == 0 {
		return nil, errors.New("The brokers list cannot be empty")
	}
	ops := NewOptions()
	for _, option := range options {
		option(ops)
	}

	logWriter := ioutil.Discard
	if verbosity >= internal.Chatty {
		logWriter = os.Stdout
	}

	sarama.Logger = log.New(logWriter, "", log.LstdFlags)

	client, err := initClient(brokers, ops)
	if err != nil {
		return nil, err
	}

	servers := client.Brokers()
	addresses := make([]string, len(servers))
	for i, broker := range servers {
		addresses[i] = broker.Addr()
	}

	admin, err := sarama.NewClusterAdmin(addresses, client.Config())
	if err != nil {
		return nil, errors.Wrap(err, "Failed to create a new cluster administrator.")
	}

	return &Manager{
		config:       ops,
		client:       client,
		Logger:       internal.NewLogger(verbosity),
		localOffsets: NewLocalOffsetManager(verbosity),
		admin:        admin,
		servers:      servers,
	}, nil
}

// GetTopics loads a list of the available topics from the server.
func (m *Manager) GetTopics(ctx context.Context, filter *regexp.Regexp, includeOffsets bool, environment string) (TopicPartitionOffset, error) {
	m.Log(internal.Verbose, "Retrieving topic list from the server")
	topics, err := m.client.Topics()
	if err != nil {
		return nil, err
	}

	result := make(TopicPartitionOffset)
	queryLocal := !internal.IsEmpty(environment)
	for _, topic := range topics {
		m.Logf(internal.SuperVerbose, "Topic %s has been found on the server", topic)
		select {
		case <-ctx.Done():
			return result, nil
		default:
			if filter != nil && !filter.Match([]byte(topic)) {
				m.Logf(internal.SuperVerbose, "Filtering out %s topic", topic)
				continue
			}
			result[topic] = make(PartitionOffset)
			if !includeOffsets {
				continue
			}
			m.Logf(internal.VeryVerbose, "Retrieving the partition(s) of %s topic from the server", topic)
			partitions, err := m.client.Partitions(topic)
			if err != nil {
				return nil, err
			}
			local := make(PartitionOffset)

			if queryLocal {
				m.Logf(internal.VeryVerbose, "Reading local offsets of %s topic", topic)
				local, err = m.localOffsets.ReadLocalTopicOffsets(topic, environment)
				if err != nil {
					return nil, err
				}
			}
			for _, partition := range partitions {
				select {
				case <-ctx.Done():
					return result, nil
				default:
					offset := newOffset()
					m.Logf(internal.SuperVerbose, "Reading the latest offset of partition %d for %s topic from the server", partition, topic)
					latestOffset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
					if err != nil {
						return nil, err
					}
					offset.Latest = latestOffset
					lo, ok := local[partition]
					if !ok && queryLocal {
						offset.Current = offsetNotFound
					}
					if ok && lo.Current >= 0 {
						offset.Current = lo.Current
					}
					result[topic][partition] = offset
				}
			}
		}
	}
	return result, nil
}

func (m *Manager) DeleteConsumerGroup(group string) error {
	return m.admin.DeleteConsumerGroup(group)
}

func (m *Manager) DeleteTopic(topic string) error {
	return m.admin.DeleteTopic(topic)
}

func (m *Manager) GetConsumerGroups(ctx context.Context, includeMembers bool, memberFilter, groupFilter *regexp.Regexp, topics []string) (ConsumerGroups, error) {
	result := make(ConsumerGroups)
	select {
	case <-ctx.Done():
		return result, nil
	default:

		m.Log(internal.Verbose, "Retrieving consumer groups from the server")
		groups, err := m.admin.ListConsumerGroups()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to fetch the consumer groups from the server.")
		}
		groupNames := make([]string, 0)
		for group := range groups {
			select {
			case <-ctx.Done():
				return result, nil
			default:
				if groupFilter != nil && !groupFilter.Match([]byte(group)) {
					continue
				}
				if includeMembers {
					groupNames = append(groupNames, group)
				}
				result[group] = &ConsumerGroup{}
			}
		}

		if len(result) == 0 {
			return result, nil
		}

		if includeMembers {
			m.Log(internal.Verbose, "Retrieving consumer group members from the server")
			groupsMeta, err := m.admin.DescribeConsumerGroups(groupNames)
			if err != nil {
				return nil, errors.Wrap(err, "Failed to retrieve the group members from the server")
			}
			for _, gm := range groupsMeta {
				select {
				case <-ctx.Done():
					return result, nil
				default:
					m.Logf(internal.VeryVerbose, "Retrieving the members of %s consumer group", gm.GroupId)
					result[gm.GroupId].addMembers(gm.Members, memberFilter)
				}
			}
		}

		if len(topics) > 0 {
			topicPartitions := make(map[string][]int32)
			m.Log(internal.Verbose, "Retrieving topic partitions from the server")
			for _, topic := range topics {
				select {
				case <-ctx.Done():
					return result, nil
				default:
					if internal.IsEmpty(topic) {
						continue
					}
					m.Logf(internal.VeryVerbose, "Retrieving the partition(s) of %s topic from the server", topic)
					partitions, err := m.client.Partitions(topic)
					if err != nil {
						return nil, err
					}
					topicPartitions[topic] = partitions
				}
			}
			err = m.setGroupOffsets(ctx, result, topicPartitions)
			if err != nil {
				return nil, err
			}
		}

		return result, nil
	}
}

func (m *Manager) setGroupOffsets(ctx context.Context, consumerGroups ConsumerGroups, topicPartitions map[string][]int32) error {
	for groupID, group := range consumerGroups {
		select {
		case <-ctx.Done():
			return nil
		default:
			m.Logf(internal.VeryVerbose, "Retrieving the offsets for %s consumer group", groupID)
			cgOffsets, err := m.admin.ListConsumerGroupOffsets(groupID, topicPartitions)
			if err != nil {
				return errors.Wrap(err, "Failed to retrieve the consumer group offsets")
			}
			group.TopicOffsets = make(TopicPartitionOffset)
			for topic, blocks := range cgOffsets.Blocks {
				for partition, block := range blocks {
					select {
					case <-ctx.Done():
						return nil
					default:
						if block.Offset < 0 {
							continue
						}
						if _, ok := group.TopicOffsets[topic]; !ok {
							// We add the topic, only if there is a group offset for one of its partitions
							group.TopicOffsets[topic] = make(PartitionOffset)
						}
						m.Logf(internal.SuperVerbose, "Retrieving the latest offset of partition %d of %s topic from the server", partition, topic)
						total, err := m.client.GetOffset(topic, partition, sarama.OffsetOldest)
						if err != nil {
							return err
						}
						group.TopicOffsets[topic][partition] = Offset{
							Current: total,
							Latest:  block.Offset,
						}
					}
				}
			}
		}
	}
	return nil
}

// GetBrokers returns the current set of active brokers as retrieved from cluster metadata.
func (m *Manager) GetBrokers(ctx context.Context, includeMetadata bool) ([]Broker, error) {
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
func (m *Manager) Close() {
	m.Logf(internal.Verbose, "Closing kafka manager.")
	err := m.admin.Close()
	if err != nil {
		m.Logf(internal.Forced, "Failed to close the cluster admin: %s", err)
	}

	err = m.client.Close()
	if err != nil {
		m.Logf(internal.Forced, "Failed to close Kafka client: %s", err)
		return
	}
	m.Logf(internal.Verbose, "Kafka manager has been closed successfully.")
}

func (m *Manager) getMetadata(broker *sarama.Broker) (*BrokerMetadata, error) {
	meta := &BrokerMetadata{
		Topics: make([]Topic, 0),
	}
	m.Logf(internal.VeryVerbose, "Connecting to broker ID #%d", broker.ID())
	if err := broker.Open(m.client.Config()); err != nil {
		return nil, err
	}
	defer func() {
		m.Logf(internal.VeryVerbose, "Closing the connection to broker ID #%d", broker.ID())
		_ = broker.Close()
	}()
	m.Logf(internal.Verbose, "Retrieving metadata for broker ID #%d", broker.ID())
	mt, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		return nil, err
	}
	meta.Version = int(mt.Version)
	for _, topic := range mt.Topics {
		if topic != nil {
			m.Logf(internal.SuperVerbose, "The metadata of topic %s has been retrieved from broker ID #%d", topic.Name, broker.ID())
			meta.Topics = append(meta.Topics, Topic{
				Name:               topic.Name,
				NumberOdPartitions: len(topic.Partitions),
			})
		}
	}
	sort.Sort(TopicsByName(meta.Topics))
	return meta, nil
}
