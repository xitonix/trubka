package kafka

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

var kafkaAPINames = map[int16]string{
	0:  "Produce",
	1:  "Fetch",
	2:  "ListOffsets",
	3:  "Metadata",
	4:  "LeaderAndIsr",
	5:  "StopReplica",
	6:  "UpdateMetadata",
	7:  "ControlledShutdown",
	8:  "OffsetCommit",
	9:  "OffsetFetch",
	10: "FindCoordinator",
	11: "JoinGroup",
	12: "Heartbeat",
	13: "LeaveGroup",
	14: "SyncGroup",
	15: "DescribeGroups",
	16: "ListGroups",
	17: "SaslHandshake",
	18: "ApiVersions",
	19: "CreateTopics",
	20: "DeleteTopics",
	21: "DeleteRecords",
	22: "InitProducerId",
	23: "OffsetForLeaderEpoch",
	24: "AddPartitionsToTxn",
	25: "AddOffsetsToTxn",
	26: "EndTxn",
	27: "WriteTxnMarkers",
	28: "TxnOffsetCommit",
	29: "DescribeAcls",
	30: "CreateAcls",
	31: "DeleteAcls",
	32: "DescribeConfigs",
	33: "AlterConfigs",
	34: "AlterReplicaLogDirs",
	35: "DescribeLogDirs",
	36: "SaslAuthenticate",
	37: "CreatePartitions",
	38: "CreateDelegationToken",
	39: "RenewDelegationToken",
	40: "ExpireDelegationToken",
	41: "DescribeDelegationToken",
	42: "DeleteGroups",
	43: "ElectLeaders",
	44: "IncrementalAlterConfigs",
	45: "AlterPartitionReassignments",
	46: "ListPartitionReassignments",
	47: "OffsetDelete",
}

// Manager a type to query Kafka metadata.
type Manager struct {
	client           sarama.Client
	admin            sarama.ClusterAdmin
	localOffsets     *LocalOffsetManager
	serversByAddress map[string]*Broker
	serversById      map[int32]*Broker
	*internal.Logger
}

// NewManager creates a new instance of Kafka manager
func NewManager(brokers []string, verbosity internal.VerbosityLevel, options ...Option) (*Manager, error) {

	logWriter := ioutil.Discard
	if verbosity >= internal.Chatty {
		logWriter = os.Stdout
	}

	sarama.Logger = log.New(logWriter, "", log.LstdFlags)

	client, err := initClient(brokers, options...)
	if err != nil {
		return nil, err
	}

	controller, err := client.Controller()
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = controller.Close()
	}()
	servers := client.Brokers()
	addresses := make([]string, len(servers))
	byAddress := make(map[string]*Broker)
	byId := make(map[int32]*Broker)
	for i, broker := range servers {
		b := NewBroker(broker, controller.ID())
		addr := broker.Addr()
		addresses[i] = addr
		byId[broker.ID()] = b
		byAddress[internal.RemovePort(addr)] = b
	}

	admin, err := sarama.NewClusterAdmin(addresses, client.Config())
	if err != nil {
		return nil, fmt.Errorf("failed to create a new cluster administrator: %w", err)
	}

	return &Manager{
		client:           client,
		Logger:           internal.NewLogger(verbosity),
		localOffsets:     NewLocalOffsetManager(verbosity),
		admin:            admin,
		serversByAddress: byAddress,
		serversById:      byId,
	}, nil
}

func (m *Manager) DeleteConsumerGroup(group string) error {
	return m.admin.DeleteConsumerGroup(group)
}

func (m *Manager) DeleteTopic(topic string) error {
	return m.admin.DeleteTopic(topic)
}

func (m *Manager) CreateTopic(topic string, partitions int32, replicationFactor int16, validateOnly bool, retention time.Duration) error {
	verb := "Creating"
	if validateOnly {
		verb = "Validating"
	}
	m.Logf(internal.Verbose, "%s %s topic, NP %d RF: %d Rms: %s", verb, topic, partitions, replicationFactor, retention)

	var config map[string]*string
	if retention > 0 {
		ms := strconv.FormatInt(retention.Milliseconds(), 10)
		config = map[string]*string{
			"retention.ms": &ms,
		}
	}
	return m.admin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     partitions,
		ReplicationFactor: replicationFactor,
		ConfigEntries:     config,
	}, validateOnly)
}

func (m *Manager) CreatePartitions(topic string, partitions int32) error {
	m.Logf(internal.Verbose, "Readjusting the partitions for %s topic. NP %d", topic, partitions)
	return m.admin.CreatePartitions(topic, partitions, nil, false)
}

func (m *Manager) DescribeCluster(ctx context.Context, includeConfig bool) (*ClusterMetadata, error) {
	m.Log(internal.Verbose, "Retrieving broker list from the server")
	result := &ClusterMetadata{
		ConfigEntries: make([]*ConfigEntry, 0),
		Brokers:       make([]*Broker, 0),
	}
	for _, broker := range m.serversByAddress {
		select {
		case <-ctx.Done():
			return result, nil
		default:
			result.Brokers = append(result.Brokers, broker)
			if includeConfig && broker.IsController {
				m.Logf(internal.Verbose, "Retrieving the cluster configuration from %s", broker.Host)
				config, err := m.loadConfig(sarama.BrokerResource, strconv.FormatInt(int64(broker.ID), 10))
				if err != nil {
					return nil, fmt.Errorf("failed to fetch the cluster configurations: %w", err)
				}
				result.ConfigEntries = config
			}
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

func (m *Manager) GetGroups(ctx context.Context, filter *regexp.Regexp, includeStates bool) ([]*ConsumerGroupDetails, error) {
	m.Log(internal.Verbose, "Retrieving consumer groups from the server")
	groupList, err := m.admin.ListConsumerGroups()
	if err != nil {
		return nil, err
	}
	result := make(map[string]*ConsumerGroupDetails)
	names := make([]string, 0)
	for group := range groupList {
		m.Logf(internal.SuperVerbose, "Consumer group %s has been found on the server", group)
		select {
		case <-ctx.Done():
			return []*ConsumerGroupDetails{}, nil
		default:
			if filter != nil && !filter.Match([]byte(group)) {
				m.Logf(internal.SuperVerbose, "Filtering out %s consumer group", group)
				continue
			}
			result[group] = &ConsumerGroupDetails{
				Name:    group,
				Members: make(GroupMembers),
			}
			if includeStates {
				names = append(names, group)
			}
		}
	}
	if includeStates {
		m.Log(internal.Verbose, "Retrieving consumer group states from the server")
		details, err := m.admin.DescribeConsumerGroups(names)
		if err != nil {
			return nil, err
		}
		for _, detail := range details {
			if group, ok := result[detail.GroupId]; ok {
				group.ProtocolType = detail.ProtocolType
				group.Protocol = detail.Protocol
				group.State = detail.State
				m.Logf(internal.VeryVerbose, "Retrieving %s group's coordinator from the server", group.Name)
				coordinator, err := m.client.Coordinator(group.Name)
				if err != nil {
					return nil, fmt.Errorf("failed to fetch the group coordinator details of %s: %w", group.Name, err)
				}
				group.Coordinator = Broker{
					Host: internal.RemovePort(coordinator.Addr()),
					ID:   coordinator.ID(),
				}
			}
		}
	}

	output := make([]*ConsumerGroupDetails, len(result))
	var i int
	for _, details := range result {
		output[i] = details
		i++
	}
	return output, nil
}

func (m *Manager) DescribeGroup(ctx context.Context, group string, includeMembers bool) (*ConsumerGroupDetails, error) {
	m.Logf(internal.Verbose, "Retrieving %s consumer group details from the server", group)
	details, err := m.admin.DescribeConsumerGroups([]string{group})
	if err != nil {
		return nil, err
	}

	result := &ConsumerGroupDetails{
		Name:    group,
		Members: make(GroupMembers),
	}
	select {
	case <-ctx.Done():
		return result, nil
	default:
		if len(details) == 0 {
			return nil, fmt.Errorf("failed to retrieve the consumer group details of %s", group)
		}
		d := details[0]
		result.State = d.State
		result.Protocol = d.Protocol
		result.ProtocolType = d.ProtocolType
		m.Logf(internal.VeryVerbose, "Retrieving %s group's coordinator from the server", group)
		coordinator, err := m.client.Coordinator(group)
		if err != nil {
			return nil, fmt.Errorf("failed to fetch the group coordinator details: %w", err)
		}
		result.Coordinator = Broker{
			Address: coordinator.Addr(),
			ID:      coordinator.ID(),
		}
		if includeMembers {
			for name, description := range d.Members {
				md, err := fromGroupMemberDescription(description)
				if err != nil {
					return nil, err
				}
				result.Members[name] = md
			}
		}
	}

	return result, nil
}

func (m *Manager) DescribeTopic(ctx context.Context, topic string, includeConfig bool) (*TopicMetadata, error) {
	m.Logf(internal.Verbose, "Retrieving %s topic details from the server", topic)
	result := &TopicMetadata{
		Partitions:    make([]*PartitionMeta, 0),
		ConfigEntries: make([]*ConfigEntry, 0),
	}
	select {
	case <-ctx.Done():
		return result, nil
	default:
		response, err := m.admin.DescribeTopics([]string{topic})
		if err != nil {
			return nil, err
		}
		if len(response) == 0 {
			return nil, fmt.Errorf("%s topic metadata was not found on the server", topic)
		}

		meta := response[0]
		for _, pm := range meta.Partitions {
			pMeta := &PartitionMeta{
				Id:              pm.ID,
				ISRs:            m.toBrokers(pm.Isr),
				Replicas:        m.toBrokers(pm.Replicas),
				OfflineReplicas: m.toBrokers(pm.OfflineReplicas),
				Leader:          m.getBrokerById(pm.Leader),
			}
			result.Partitions = append(result.Partitions, pMeta)
		}

		if includeConfig {
			m.Logf(internal.Verbose, "Retrieving %s topic configurations from the server", topic)
			config, err := m.loadConfig(sarama.TopicResource, topic)
			if err != nil {
				return nil, fmt.Errorf("failed to fetch the topic configurations: %w", err)
			}
			result.ConfigEntries = config
		}
	}

	return result, nil
}

func (m *Manager) DescribeBroker(ctx context.Context, addressOrId string, includeLogs, includeAPIVersions bool, topicFilter *regexp.Regexp) (*BrokerMeta, error) {
	meta := &BrokerMeta{
		Logs: make([]*LogFile, 0),
		APIs: make([]*API, 0),
	}
	select {
	case <-ctx.Done():
		return meta, nil
	default:
		broker, err := m.findBroker(addressOrId)
		if err != nil {
			return nil, err
		}
		meta.IsController = broker.IsController
		m.Logf(internal.Verbose, "Connecting to %s", addressOrId)
		err = broker.Open(m.client.Config())
		if err != nil {
			return nil, fmt.Errorf("failed to connect to %s: %w", addressOrId, err)
		}
		defer func() {
			_ = broker.Close()
		}()

		m.Logf(internal.Verbose, "Fetching consumer groups from broker %s", addressOrId)
		response, err := broker.ListGroups(&sarama.ListGroupsRequest{})
		if err != nil {
			return nil, err
		}
		meta.ConsumerGroups = make([]string, len(response.Groups))
		var i int
		for group := range response.Groups {
			m.Logf(internal.VeryVerbose, "Consumer group %s has been retrieved from broker %s", group, addressOrId)
			meta.ConsumerGroups[i] = group
			i++
		}

		if includeAPIVersions {
			m.Logf(internal.VeryVerbose, "Fetching the API versions from broker %s", addressOrId)
			apiResponse, err := broker.ApiVersions(&sarama.ApiVersionsRequest{})
			if err != nil {
				return nil, fmt.Errorf("failed to retrieve the API versions from %s: %w", addressOrId, err)
			}
			for _, api := range apiResponse.ApiVersions {
				m.Logf(internal.Chatty, "API key %d retrieved from broker %s", api.ApiKey, addressOrId)
				name := kafkaAPINames[api.ApiKey]
				meta.APIs = append(meta.APIs, newAPI(name, api.ApiKey, api.MinVersion, api.MaxVersion))
			}
		}

		if includeLogs {
			m.Logf(internal.VeryVerbose, "Retrieving broker log details from broker %s", addressOrId)
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

func (m *Manager) toBrokers(ids []int32) []*Broker {
	result := make([]*Broker, len(ids))
	for i := 0; i < len(ids); i++ {
		result[i] = m.getBrokerById(ids[i])
	}
	return result
}

func (m *Manager) getBrokerById(id int32) *Broker {
	if b, ok := m.serversById[id]; ok {
		return b
	}
	return &Broker{
		ID: id,
	}
}

func (m *Manager) findBroker(idOrAddress string) (*Broker, error) {
	if b, ok := m.serversByAddress[idOrAddress]; ok {
		return b, nil
	}

	id, err := strconv.ParseInt(idOrAddress, 10, 32)
	if err != nil {
		return nil, errors.New("invalid broker Id. The broker Id must be an integer")
	}
	if b, ok := m.serversById[int32(id)]; ok {
		return b, nil
	}

	return nil, fmt.Errorf("broker %v not found", idOrAddress)
}

func (m *Manager) loadConfig(resourceType sarama.ConfigResourceType, resourceName string) ([]*ConfigEntry, error) {
	entries, err := m.admin.DescribeConfig(sarama.ConfigResource{
		Type:        resourceType,
		Name:        resourceName,
		ConfigNames: nil,
	})

	if err != nil {
		return nil, err
	}

	result := make([]*ConfigEntry, 0)
	for _, entry := range entries {
		if internal.IsEmpty(entry.Value) {
			continue
		}
		result = append(result, &ConfigEntry{
			Name:  entry.Name,
			Value: entry.Value,
		})
	}
	return result, nil
}
