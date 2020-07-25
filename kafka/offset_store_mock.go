package kafka

type offsetStoreMock struct {
	err              chan error
	tpo              TopicPartitionOffset
	forceReadFailure bool
}

func newOffsetStoreMock(forceReadFailure bool) *offsetStoreMock {
	return &offsetStoreMock{
		err:              make(chan error),
		tpo:              make(TopicPartitionOffset),
		forceReadFailure: forceReadFailure,
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
	if o.forceReadFailure {
		return nil, errDeliberate
	}
	return o.tpo[topic], nil
}

func (o *offsetStoreMock) errors() <-chan error {
	return o.err
}

func (o *offsetStoreMock) close() {
}

func (o *offsetStoreMock) set(topic string, po map[int32]int64) {
	o.tpo[topic] = make(PartitionOffset)
	for partition, offset := range po {
		o.tpo[topic][partition] = Offset{
			Current: offset,
		}
	}
}
