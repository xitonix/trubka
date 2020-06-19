package kafka

import (
	"sort"
	"strconv"
	"strings"
)

type TopicPartitions map[string][]int32

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
