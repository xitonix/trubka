package kafka

import (
	"io"

	"github.com/Shopify/sarama"
)

type client interface {
	Partitions(topic string) ([]int32, error)
	ConsumePartition(topic string, partition int32, offset int64) (sarama.PartitionConsumer, error)
	Topics() ([]string, error)
	GetOffset(topic string, partitionID int32, time int64) (int64, error)
	io.Closer
}
