package kafka

import "sort"

// TopicPartitionOffset represents a map of topic offset pairs for all the partitions.
type TopicPartitionOffset map[string]PartitionOffset

func (t TopicPartitionOffset) ToJson() interface{} {
	if t == nil {
		return nil
	}
	type offset struct {
		Current int64 `json:"current_offset"`
		Latest  int64 `json:"latest_offset"`
		Lag     int64 `json:"lag"`
	}
	type tpo struct {
		Topic      string           `json:"topic"`
		Partitions map[int32]offset `json:"partitions"`
	}
	output := make([]tpo, len(t))
	i := 0
	for topic, po := range t {
		topicOffsets := tpo{
			Topic:      topic,
			Partitions: make(map[int32]offset, len(po)),
		}
		for partition, off := range po {
			topicOffsets.Partitions[partition] = offset{
				Current: off.Current,
				Latest:  off.Latest,
				Lag:     off.Lag(),
			}
		}
		output[i] = topicOffsets
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
