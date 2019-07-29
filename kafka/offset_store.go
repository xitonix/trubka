package kafka

import "io"

// OffsetStore is the interface to store partition offsets.
type OffsetStore interface {
	io.Closer
	Store(topic string, partition int32, offset int64) error
	Query(topic string) (map[int32]int64, error)
}
