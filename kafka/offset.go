package kafka

import "strconv"

const unknownOffset int64 = -3
const offsetNotFound int64 = -4

// Offset represents an offset pair for a given partition.
//
// A pair contains the latest offset of the partition reported by the server and the local or consumer group offset.
type Offset struct {
	// Latest the latest available offset of the partition reported by the server.
	Latest int64
	// Current the current value of the local or consumer group offset. This is where the consumer up to.
	Current int64
}

// Lag calculates the lag between the latest and the current offset values.
func (o Offset) Lag() int64 {
	if o.Latest > o.Current {
		return o.Latest - o.Current
	}
	return 0
}

// String returns the string representation of the given offset.
func (o Offset) String(latest bool) string {
	if latest {
		return getOffsetText(o.Latest)
	}
	return getOffsetText(o.Current)
}

func getOffsetText(offset int64) string {
	switch offset {
	case unknownOffset, offsetNotFound:
		return "-"
	default:
		return strconv.FormatInt(offset, 10)
	}
}
