package kafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

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
	if offset < -2 {
		offset = -2
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

// Offset returns the final offset value from which consuming will be started.
//
// In MillisecondsOffsetMode, the offset will be the milliseconds of the specified time.
// This is what Kafka needs to figure out the closest available offset at the given time.
func (c *Checkpoint) Offset() int64 {
	return c.offset
}

// TimeOffset returns the originally provided time value of the time-based offset in MillisecondsOffsetMode mode.
func (c *Checkpoint) TimeOffset() time.Time {
	return c.at
}

// OffsetString returns the string representation of the time offset in `02-01-2006T15:04:05.999999999` format if in
// MillisecondsOffsetMode mode, otherwise returns the string representation of the offset value.
func (c *Checkpoint) OffsetString() string {
	if c.isTimeBased {
		return internal.FormatTimeUTC(c.at)
	}
	switch c.offset {
	case sarama.OffsetNewest:
		return "newest"
	case sarama.OffsetOldest:
		return "oldest"
	default:
		return strconv.FormatInt(c.offset, 10)
	}
}
