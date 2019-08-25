package kafka

const UnknownOffset int64 = -3

type OffsetPair struct {
	Local  int64
	Remote int64
}

func newOffsetPair() OffsetPair {
	return OffsetPair{
		Local:  UnknownOffset,
		Remote: UnknownOffset,
	}
}

type PartitionsOffsetPair map[int32]OffsetPair
