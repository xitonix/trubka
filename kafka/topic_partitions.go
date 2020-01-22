package kafka

import (
	"sort"
	"strconv"
	"strings"
)

type TopicPartitions map[string][]int32

func (t TopicPartitions) SortedPartitionsString(topic string) string {
	partitions, ok := t[topic]
	if !ok {
		return ""
	}
	partStr := make([]string, len(partitions))
	for i := 0; i < len(partitions); i++ {
		partStr[i] = strconv.FormatInt(int64(partitions[i]), 10)
	}
	sort.Strings(partStr)
	return strings.Join(partStr, ",")
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
