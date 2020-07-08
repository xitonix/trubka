package kafka

import "fmt"

// Topic represents a Kafka topic with metadata.
type Topic struct {
	// Name topic name.
	Name string `json:"name"`
	// NumberOfPartitions number of partitions within the topic.
	NumberOfPartitions int32 `json:"number_of_partitions"`
	// ReplicationFactor replication factor.
	ReplicationFactor int16 `json:"replication_factor"`
}

// TopicsByName sorts the topic list by name.
type TopicsByName []Topic

func (t TopicsByName) Len() int {
	return len(t)
}

func (t TopicsByName) Swap(i, j int) {
	t[i], t[j] = t[j], t[i]
}

func (t TopicsByName) Less(i, j int) bool {
	return t[i].Name < t[j].Name
}

// GetNames returns a list of all the topic names.
func (t TopicsByName) GetNames() []string {
	result := make([]string, len(t))
	for i, topic := range t {
		result[i] = topic.Name
	}
	return result
}

// String returns the string representation of the topic metadata.
func (t Topic) String() string {
	return fmt.Sprintf("%s (Partitions: %d, Replication Factor: %d)", t.Name, t.NumberOfPartitions, t.ReplicationFactor)
}
