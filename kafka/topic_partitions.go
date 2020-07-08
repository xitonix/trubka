package kafka

import (
	"sort"
	"strconv"
	"strings"
)

// TopicPartitions a map of topic and partition list.
type TopicPartitions map[string][]int32

// SortedPartitions returns a list of all the partitions sorted in ascending order.
func (t TopicPartitions) SortedPartitions(topic string) []int {
	partitions, ok := t[topic]
	if !ok {
		return []int{}
	}

	sorted := make([]int, len(partitions))
	for i := 0; i < len(partitions); i++ {
		sorted[i] = int(partitions[i])
	}
	sort.Ints(sorted)
	return sorted
}

// SortedPartitionsString returns a comma separated string of the sorted partitions.
func (t TopicPartitions) SortedPartitionsString(topic string) string {
	sorted := t.SortedPartitions(topic)
	if len(sorted) == 0 {
		return ""
	}
	partitions := make([]string, len(sorted))
	for i, p := range sorted {
		partitions[i] = strconv.Itoa(p)
	}
	return strings.Join(partitions, ",")
}

// SortedTopics returns a list of all the topics in the map sorted alphabetically.
func (t TopicPartitions) SortedTopics() []string {
	if len(t) == 0 {
		return []string{}
	}
	topics := make([]string, len(t))
	var i int
	for topic := range t {
		topics[i] = topic
		i++
	}
	sort.Strings(topics)
	return topics
}
