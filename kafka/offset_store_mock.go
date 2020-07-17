package kafka

type offsetStoreMock struct {
	err chan error
	tpo TopicPartitionOffset
}

func newOffsetStoreMock() *offsetStoreMock {
	return &offsetStoreMock{
		err: make(chan error),
		tpo: make(TopicPartitionOffset),
	}
}

func (o *offsetStoreMock) start(loaded TopicPartitionOffset) {
	o.tpo = loaded
}

func (o *offsetStoreMock) commit(topic string, partition int32, offset int64) error {
	o.tpo[topic][partition] = Offset{
		Current: offset,
	}
	return nil
}

func (o *offsetStoreMock) read(topic string) (PartitionOffset, error) {
	return o.tpo[topic], nil
}

func (o *offsetStoreMock) errors() <-chan error {
	return o.err
}

func (o *offsetStoreMock) close() {
}
