package kafka

// TopicMetadata holds metadata for a topic.
type TopicMetadata struct {
	// Partitions a list of all the partitions.
	Partitions []*PartitionMeta `json:"partitions,omitempty"`
	// ConfigEntries a list of topic configurations stored on the server.
	ConfigEntries []*ConfigEntry `json:"configurations,omitempty"`
}
