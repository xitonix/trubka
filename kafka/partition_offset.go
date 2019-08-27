package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
)

// PartitionOffsets represents a map of partitions and offsets
type PartitionOffsets map[int32]int64

// serialises the offset map and returns the bytes as well as the checksum string of the current values.
func (p PartitionOffsets) marshal() (string, []byte, error) {
	if len(p) == 0 {
		return "", []byte{}, nil
	}
	toWrite := make(PartitionOffsets)
	for pt, of := range p {
		if of >= 0 {
			toWrite[pt] = of
		}
	}
	if len(toWrite) == 0 {
		return "", nil, nil
	}
	buff := bytes.Buffer{}
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(toWrite)
	if err != nil {
		return "", nil, err
	}
	return strings.Replace(fmt.Sprintf("%v", toWrite), "map", "", 1), buff.Bytes(), nil
}

func (p PartitionOffsets) SortedPartitions() []int {
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

type TopicPartitionOffset map[string]PartitionOffsets

func (t TopicPartitionOffset) SortedTopics() []string {
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

func (p PartitionOffsets) copyTo(dest PartitionOffsets) {
	if len(p) == 0 {
		return
	}
	if dest == nil {
		dest = make(PartitionOffsets)
	}
	for partition, offset := range p {
		if offset >= 0 {
			dest[partition] = offset
		}
	}
}
