package kafka

type inMemoryOffsetStore struct {
	m map[string]map[int32]int64
}

func newInMemoryStore() *inMemoryOffsetStore {
	return &inMemoryOffsetStore{
		m: make(map[string]map[int32]int64),
	}
}

func (s *inMemoryOffsetStore) store(topic string, partition int32, offset int64) {
	_, ok := s.m[topic]
	if !ok {
		s.m[topic] = make(map[int32]int64)
	}
	s.m[topic][partition] = offset
}

func (s *inMemoryOffsetStore) resetTo(topic string, partition int32, offset int64) {
	tp, ok := s.m[topic]
	if !ok {
		return
	}
	tp[partition] = offset
}

func (s *inMemoryOffsetStore) getOffsets(topic string) map[int32]int64 {
	return s.m[topic]
}

func (s *inMemoryOffsetStore) getPartitionOffset(topic string, partition int32) int64 {
	tp, ok := s.m[topic]
	if !ok {
		return -1
	}
	return tp[partition]
}

func (s *inMemoryOffsetStore) close() error {
	return nil
}
