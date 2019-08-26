package kafka

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"io/ioutil"
	"log"
	"os"
	"regexp"
	"sort"
	"time"

	"github.com/Shopify/sarama"
	"github.com/kirsle/configdir"
	"github.com/peterbourgon/diskv"
	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
)

// Manager a type to query Kafka metadata.
type Manager struct {
	config       *Options
	client       sarama.Client
	currentLevel internal.VerbosityLevel
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
		currentLevel: verbosity,
	}, nil
}

// GetTopics loads a list of the available topics from the server.
func (m *Manager) GetTopics(ctx context.Context, filter *regexp.Regexp, includeOffsets bool, environment string) (map[string]PartitionsOffsetPair, error) {
	m.log(internal.Verbose, "Retrieving topic list from the server")
	topics, err := m.client.Topics()
	result := make(map[string]PartitionsOffsetPair)
	if err != nil {
		return nil, err
	}

	for _, topic := range topics {
		m.logf(internal.SuperVerbose, "Topic %s has been found on the server", topic)
		select {
		case <-ctx.Done():
			return result, nil
		default:
			if filter != nil && !filter.Match([]byte(topic)) {
				m.logf(internal.SuperVerbose, "Filtering out %s topic", topic)
				continue
			}
			result[topic] = make(PartitionsOffsetPair)
			if !includeOffsets {
				continue
			}
			m.logf(internal.VeryVerbose, "Retrieving the partition(s) of %s topic from the server", topic)
			partitions, err := m.client.Partitions(topic)
			if err != nil {
				return nil, err
			}
			local := make(PartitionOffsets)
			loadLocal := !internal.IsEmpty(environment)
			if loadLocal {
				m.logf(internal.VeryVerbose, "Reading local offsets of %s topic", topic)
				local, err = readLocalOffset(topic, environment)
				if err != nil {
					return nil, err
				}
			}
			for _, partition := range partitions {
				op := newOffsetPair()
				m.logf(internal.SuperVerbose, "Reading the latest offset of partition %d for %s topic from the server", partition, topic)
				offset, err := m.client.GetOffset(topic, partition, sarama.OffsetNewest)
				if err != nil {
					return nil, err
				}
				op.Remote = offset
				lo, ok := local[partition]
				if !ok && loadLocal {
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

// GetBrokers returns the current set of active brokers as retrieved from cluster metadata.
func (m *Manager) GetBrokers(ctx context.Context, includeMetadata bool) ([]Broker, error) {
	m.log(internal.Verbose, "Retrieving broker list from the server")
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
	m.logf(internal.VeryVerbose, "Connecting to broker ID #%d", broker.ID())
	if err := broker.Open(m.client.Config()); err != nil {
		return nil, err
	}
	defer func() {
		m.logf(internal.VeryVerbose, "Closing the connection to broker ID #%d", broker.ID())
		_ = broker.Close()
	}()
	m.logf(internal.Verbose, "Retrieving metadata for broker ID #%d", broker.ID())
	mt, err := broker.GetMetadata(&sarama.MetadataRequest{})
	if err != nil {
		return nil, err
	}
	meta.Version = int(mt.Version)
	for _, topic := range mt.Topics {
		if topic != nil {
			m.logf(internal.SuperVerbose, "The metadata of topic %s has been retrieved from broker ID #%d", topic.Name, broker.ID())
			meta.Topics = append(meta.Topics, Topic{
				Name:               topic.Name,
				NumberOdPartitions: len(topic.Partitions),
			})
		}
	}
	sort.Sort(TopicsByName(meta.Topics))
	return meta, nil
}

func (m *Manager) log(level internal.VerbosityLevel, message string) {
	if m.currentLevel < level {
		return
	}
	fmt.Println(time.Now().Format("2006/01/02 15:04:05 ") + message)
}

func (m *Manager) logf(level internal.VerbosityLevel, format string, a ...interface{}) {
	m.log(level, fmt.Sprintf(format, a...))
}

func readLocalOffset(topic string, environment string) (PartitionOffsets, error) {
	result := make(PartitionOffsets)
	root := configdir.LocalConfig("trubka", environment)
	flatTransform := func(s string) []string { return []string{} }

	db := diskv.New(diskv.Options{
		BasePath:     root,
		Transform:    flatTransform,
		CacheSizeMax: 1024 * 1024,
	})

	val, err := db.Read(topic)
	if err != nil {
		if os.IsNotExist(err) {
			return result, nil
		}
		return nil, err
	}

	buff := bytes.NewBuffer(val)
	dec := gob.NewDecoder(buff)
	err = dec.Decode(&result)
	if err != nil {
		return nil, errors.Wrapf(err, "Failed to deserialize the value from local offset store for topic %s", topic)
	}

	return result, nil
}
