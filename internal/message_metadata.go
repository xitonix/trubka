package internal

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"
)

const (
	topicPrefix     = "Topic"
	partitionPrefix = "Partition"
	offsetPrefix    = "Offset"
	keyPrefix       = "Key"
	timePrefix      = "Time"
)

// MessageMetadata represents the message metadata which should be included in the output.
type MessageMetadata struct {
	// Partition enables printing partition to the output.
	Partition bool
	// Offset enables printing offset to the output.
	Offset bool
	// Key enable printing partition key to the output.
	Key bool
	// Timestamp enabled printing message timestamp to the output.
	Timestamp bool
	// Topic enabled printing topic name to the output.
	Topic bool

	maxPrefixLength int
}

// IsRequested returns true if any piece of metadata has been requested by the user to be included in the output.
func (m *MessageMetadata) IsRequested() bool {
	return m.Timestamp || m.Key || m.Offset || m.Partition || m.Topic
}

// Render prepends the requested metadata to the message.
func (m *MessageMetadata) Render(key, message []byte, ts time.Time, topic string, partition int32, offset int64, b64 bool) []byte {
	if m.Timestamp {
		message = m.prependTimestamp(ts, message)
	}

	if m.Key {
		message = m.prependKey(key, message, b64)
	}

	if m.Offset {
		message = m.prependOffset(offset, message)
	}

	if m.Partition {
		message = m.prependPartition(partition, message)
	}

	if m.Topic {
		message = m.prependTopic(topic, message)
	}

	return message
}

// SetIndentation sets the indentation for metadata.
func (m *MessageMetadata) SetIndentation() {
	// MAKE sure the if clauses are in descending order by prefix length.
	if m.Partition {
		m.maxPrefixLength = len(partitionPrefix)
		return
	}

	if m.Offset {
		m.maxPrefixLength = len(offsetPrefix)
		return
	}

	if m.Topic {
		m.maxPrefixLength = len(topicPrefix)
		return
	}

	if m.Timestamp {
		m.maxPrefixLength = len(timePrefix)
		return
	}

	if m.Key {
		m.maxPrefixLength = len(keyPrefix)
	}
}

func (m *MessageMetadata) getPrefix(prefix string) string {
	indentation := m.maxPrefixLength - len(prefix)
	if indentation > 0 {
		return fmt.Sprintf("%s%s", strings.Repeat(" ", indentation), prefix)
	}
	return prefix
}

func (m *MessageMetadata) prependTimestamp(ts time.Time, in []byte) []byte {
	return append([]byte(fmt.Sprintf("%s: %s\n", m.getPrefix(timePrefix), FormatTime(ts))), in...)
}

func (m *MessageMetadata) prependTopic(topic string, in []byte) []byte {
	return append([]byte(fmt.Sprintf("%s: %s\n", m.getPrefix(topicPrefix), topic)), in...)
}

func (m *MessageMetadata) prependKey(key []byte, in []byte, b64 bool) []byte {
	prefix := m.getPrefix(keyPrefix)
	if b64 {
		return append([]byte(fmt.Sprintf("%s: %s\n", prefix, base64.StdEncoding.EncodeToString(key))), in...)
	}
	return append([]byte(fmt.Sprintf("%s: %X\n", prefix, key)), in...)
}

func (m *MessageMetadata) prependOffset(offset int64, in []byte) []byte {
	return append([]byte(fmt.Sprintf("%s: %d\n", m.getPrefix(offsetPrefix), offset)), in...)
}

func (m *MessageMetadata) prependPartition(partition int32, in []byte) []byte {
	return append([]byte(fmt.Sprintf("%s: %d\n", m.getPrefix(partitionPrefix), partition)), in...)
}
