package kafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

// CheckpointPair represents a pair of From/To checkpoints.
type CheckpointPair struct {
	// From Specifies where to start consuming from.
	From *Checkpoint
	// To Specifies when the consumer must stop.
	To *Checkpoint
}

// Checkpoint represents a point in time or offset, from which the consumer has to start consuming from the specified topic.
type Checkpoint struct {
	offset      int64
	at          time.Time
	isTimeBased bool
}

func newCheckpoint(rewind bool) *Checkpoint {
	offset := sarama.OffsetNewest
	if rewind {
		offset = sarama.OffsetOldest
	}
	return &Checkpoint{
		offset: offset,
	}
}

func newOffsetCheckpoint(offset int64) *Checkpoint {
	if offset < sarama.OffsetOldest {
		offset = sarama.OffsetOldest
	}
	return &Checkpoint{
		offset: offset,
	}
}

// newTimeCheckpoint creates a new checkpoint and sets the offset to the milliseconds of the given time and the mode to MillisecondsOffsetMode.
func newTimeCheckpoint(at time.Time) *Checkpoint {
	var offsetMilliSeconds int64
	switch {
	case at.IsZero():
		offsetMilliSeconds = sarama.OffsetOldest
	default:
		offsetMilliSeconds = at.UnixNano() / 1000000
	}
	return &Checkpoint{
		isTimeBased: true,
		offset:      offsetMilliSeconds,
		at:          at,
	}
}

// OffsetString returns the string representation of the time offset in `02-01-2006T15:04:05.999999999` format if in
// MillisecondsOffsetMode mode, otherwise returns the string representation of the offset value.
func (c *Checkpoint) OffsetString() string {
	if c.isTimeBased {
		return internal.FormatTimeForHuman(c.at)
	}
	switch c.offset {
	case sarama.OffsetNewest:
		return "'newest'"
	case sarama.OffsetOldest:
		return "'oldest'"
	default:
		return strconv.FormatInt(c.offset, 10)
	}
}

func (c *Checkpoint) after(cp *Checkpoint) bool {
	if c == nil || cp == nil {
		return false
	}

	if c.isTimeBased {
		if cp.isTimeBased {
			return c.at.After(cp.at)
		}
		return false
	}

	return c.offset > cp.offset
}
