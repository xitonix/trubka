package kafka

type TopicMetadata struct {
	Partitions    []*PartitionMeta
	ConfigEntries []*ConfigEntry
}
