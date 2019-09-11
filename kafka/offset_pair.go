package kafka

import (
	"sort"
	"strconv"
)

const unknownOffset int64 = -3
const offsetNotFound int64 = -4

// OffsetPair represents a pair of local/remote offset pairs for a given partition.
type OffsetPair struct {
	// Local locally stored offset.
	Local int64
	// Remote the latest partition offset fetched from Kafka.
	Remote int64
}

func newOffsetPair() OffsetPair {
	return OffsetPair{
		Local:  unknownOffset,
		Remote: unknownOffset,
	}
}

// LocalString returns the string representation of the local offset.
func (o OffsetPair) LocalString() string {
	return getOffsetText(o.Local)
}

// RemoteString returns the string representation of the remote offset.
func (o OffsetPair) RemoteString() string {
	return getOffsetText(o.Remote)
}

func getOffsetText(offset int64) string {
	switch offset {
	case unknownOffset:
		return "-"
	case offsetNotFound:
		return "N/A"
	default:
		return strconv.FormatInt(offset, 10)
	}
}

// PartitionsOffsetPair represents a map of partition offset pairs.
type PartitionsOffsetPair map[int32]OffsetPair

// SortedPartitions returns a list of sorted partitions.
func (p PartitionsOffsetPair) SortedPartitions() []int {
	sorted := make([]int, 0)
	if len(p) == 0 {
		return sorted
	}
	for partition := range p {
		sorted = append(sorted, int(partition))
	}
	sort.Ints(sorted)
	return sorted
}

// TopicPartitionOffsetPairs represents a map of topic offset pairs for all the partitions.
type TopicPartitionOffsetPairs map[string]PartitionsOffsetPair

// SortedTopics returns a list of sorted topics.
func (t TopicPartitionOffsetPairs) SortedTopics() []string {
	sorted := make([]string, 0)
	if len(t) == 0 {
		return sorted
	}
	for topic := range t {
		sorted = append(sorted, topic)
	}
	sort.Strings(sorted)
	return sorted
}
