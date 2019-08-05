package kafka

// OffsetStore is the interface to store partition offsets.
type OffsetStore interface {
	Store(topic string, partition int32, offset int64) error
	Query(topic string) (PartitionOffsets, error)
}
