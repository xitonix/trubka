package kafka

import "io"

type OffsetStore interface {
	io.Closer
	Store(topic string, partition int32, offset int64) error
	Query(topic string) (map[int32]int64, error)
}
