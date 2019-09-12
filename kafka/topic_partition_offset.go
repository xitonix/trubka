package kafka

import "sort"

// TopicPartitionOffsetPair represents a map of topic offset pairs for all the partitions.
type TopicPartitionOffset map[string]PartitionOffset

// SortedTopics returns a list of sorted topics.
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

// ToTopicPartitionOffset creates a new TopicPartitionOffset from a raw map.
//
// Set latest parameter to true, if you would like to set the Latest offset value instead of the Current value.
func ToTopicPartitionOffset(tpo map[string]map[int32]int64, latest bool) TopicPartitionOffset {
	result := make(TopicPartitionOffset)
	for topic, po := range tpo {
		result[topic] = ToPartitionOffset(po, latest)
	}
	return result
}
