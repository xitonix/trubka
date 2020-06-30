package kafka

type TopicMetadata struct {
	Partitions    []*PartitionMeta `json:"partitions,omitempty"`
	ConfigEntries []*ConfigEntry   `json:"configurations,omitempty"`
}
