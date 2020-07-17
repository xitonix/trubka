package kafka

type offsetStore interface {
	start(loaded TopicPartitionOffset)
	commit(topic string, partition int32, offset int64) error
	read(topic string) (PartitionOffset, error)
	errors() <-chan error
	close()
}
