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
	localOffsets *LocalOffsetManager
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

	return &Manager{
		config:       ops,
		client:       client,
		Logger:       internal.NewLogger(verbosity),
		localOffsets: NewLocalOffsetManager(verbosity),
	}, nil
}

// GetTopics loads a list of the available topics from the server.
func (m *Manager) GetTopics(ctx context.Context, filter *regexp.Regexp, includeOffsets bool, environment string) (TopicPartitionOffsetPairs, error) {
	m.Log(internal.Verbose, "Retrieving topic list from the server")
	topics, err := m.client.Topics()
	result := make(TopicPartitionOffsetPairs)
	if err != nil {
		return nil, err
	}

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
			result[topic] = make(PartitionsOffsetPair)
			if !includeOffsets {
				continue
			}
			m.Logf(internal.VeryVerbose, "Retrieving the partition(s) of %s topic from the server", topic)
			partitions, err := m.client.Partitions(topic)
			if err != nil {
				return nil, err
			}
			local := make(PartitionOffsets)

			if queryLocal {
				m.Logf(internal.VeryVerbose, "Reading local offsets of %s topic", topic)
				local, err = m.localOffsets.ReadLocalTopicOffsets(topic, environment)
				if err != nil {
					return nil, err
				}
			}
			for _, partition := range partitions {
				op := newOffsetPair()
				m.Logf(internal.SuperVerbose, "Reading the latest offset of partition %d for %s topic from the server", partition, topic)
				offset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					return nil, err
				}
				op.Remote = offset
				lo, ok := local[partition]
				if !ok && queryLocal {
					op.Local = offsetNotFound
				}
				if ok && lo >= 0 {
					op.Local = lo
				}
				result[topic][partition] = op
			}
		}
	}
	return result, nil
}

func (m *Manager) GetConsumerGroups(ctx context.Context, includeMembers bool, memberFilter *regexp.Regexp, topics []string) (ConsumerGroups, error) {
	result := make(ConsumerGroups)
	select {
	case <-ctx.Done():
		return result, nil
	default:
		addresses := m.getBrokerAddresses(ctx)
		admin, err := sarama.NewClusterAdmin(addresses, m.client.Config())
		if err != nil {
			return nil, errors.Wrap(err, "Failed to create a new cluster administrator.")
		}

		m.Log(internal.Verbose, "Retrieving consumer groups from the server")
		groups, err := admin.ListConsumerGroups()
		if err != nil {
			return nil, errors.Wrap(err, "Failed to fetch the consumer groups from the server.")
		}

		groupNames := make([]string, 0)
		for group := range groups {
			select {
			case <-ctx.Done():
				return result, nil
			default:
				if includeMembers {
					groupNames = append(groupNames, group)
				}
				result[group] = &ConsumerGroup{}
			}
		}

		if includeMembers {
			m.Log(internal.Verbose, "Retrieving consumer group members from the server")
			groupsMeta, err := admin.DescribeConsumerGroups(groupNames)
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
			totals := make(map[string]int64)
			// TODO: Calculate Totals for each partition.
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
			for groupID, cg := range result {
				select {
				case <-ctx.Done():
					return result, nil
				default:
					m.Logf(internal.VeryVerbose, "Retrieving the offsets for %s consumer group", groupID)
					cgOffsets, err := admin.ListConsumerGroupOffsets(groupID, topicPartitions)
					if err != nil {
						return nil, errors.Wrap(err, "Failed to retrieve the consumer group offsets")
					}
					cg.GroupOffsets = make(map[string]GroupOffset)
					for t, pOffsets := range cgOffsets.Blocks {
						var current int64
						for _, grpOffset := range pOffsets {
							if grpOffset.Offset > 0 {
								current += grpOffset.Offset
							}
						}
						cg.GroupOffsets[t] = GroupOffset{
							Latest:  totals[t],
							Current: current,
						}
					}
				}
			}
		}

		return result, nil
	}
}

func (m *Manager) getBrokerAddresses(ctx context.Context) []string {
	m.Log(internal.Verbose, "Retrieving broker list from the server")
	brokers := m.client.Brokers()
	addresses := make([]string, len(brokers))
	for i, broker := range brokers {
		select {
		case <-ctx.Done():
			return addresses
		default:
			addresses[i] = broker.Addr()
		}
	}
	return addresses
}

// GetBrokers returns the current set of active brokers as retrieved from cluster metadata.
func (m *Manager) GetBrokers(ctx context.Context, includeMetadata bool) ([]Broker, error) {
	m.Log(internal.Verbose, "Retrieving broker list from the server")
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
