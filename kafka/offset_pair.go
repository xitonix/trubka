package kafka

import "strconv"

const unknownOffset int64 = -3
const offsetNotFound int64 = -4

type OffsetPair struct {
	Local  int64
	Remote int64
}

func newOffsetPair() OffsetPair {
	return OffsetPair{
		Local:  unknownOffset,
		Remote: unknownOffset,
	}
}

func (o OffsetPair) LocalString() string {
	return getOffsetText(o.Local)
}

func (o OffsetPair) RemoteString() string {
	return getOffsetText(o.Remote)
}

func getOffsetText(offset int64) string {
	switch offset {
	case unknownOffset:
		return "-"
	case offsetNotFound:
		return "N/A"
	default:
		return strconv.FormatInt(offset, 10)
	}
}

type PartitionsOffsetPair map[int32]OffsetPair
