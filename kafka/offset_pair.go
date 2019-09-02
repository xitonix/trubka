package kafka

import (
	"sort"
	"strconv"
)

const unknownOffset int64 = -3
const offsetNotFound int64 = -4

type OffsetPair struct {
	Local  int64
	Remote int64
}

func newOffsetPair() OffsetPair {
	return OffsetPair{
		Local:  unknownOffset,
		Remote: unknownOffset,
	}
}

func (o OffsetPair) LocalString() string {
	return getOffsetText(o.Local)
}

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

type PartitionsOffsetPair map[int32]OffsetPair

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

func (p PartitionsOffsetPair) getPartitions() ([]int32, int64) {
	var total int64
	result := make([]int32, len(p))
	var i int
	for p, offset := range p {
		result[i] = p
		if offset.Remote > 0 {
			total += offset.Remote
		}
		i++
	}
	return result, total
}

type TopicPartitionOffsetPairs map[string]PartitionsOffsetPair

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
