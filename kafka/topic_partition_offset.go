package kafka

import "sort"

// TopicPartitionOffset represents a map of topic offset pairs for all the partitions.
type TopicPartitionOffset map[string]PartitionOffset

// ToJson returns an object ready to be serialised into json string.
func (t TopicPartitionOffset) ToJson() interface{} {
	if t == nil {
		return nil
	}

	type tpo struct {
		Topic      string      `json:"topic"`
		Partitions interface{} `json:"partitions"`
	}
	output := make([]tpo, len(t))
	i := 0
	for topic, po := range t {
		output[i] = tpo{
			Topic:      topic,
			Partitions: po.ToJson(),
		}
		i++
	}
	return output
}

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
