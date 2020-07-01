package kafka

import "fmt"

type Topic struct {
	Name               string `json:"name"`
	NumberOfPartitions int32  `json:"number_of_partitions"`
	ReplicationFactor  int16  `json:"replication_factor"`
}

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

func (t TopicsByName) GetNames() []string {
	result := make([]string, len(t))
	for i, topic := range t {
		result[i] = topic.Name
	}
	return result
}

func (t Topic) String() string {
	return fmt.Sprintf("%s (Partitions: %d, Replication Factor: %d)", t.Name, t.NumberOfPartitions, t.ReplicationFactor)
}
