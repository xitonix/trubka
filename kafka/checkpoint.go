package kafka

import (
	"strconv"
	"time"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

// OffsetMode represents the offset mode for a checkpoint.
type OffsetMode int8

const (
	// UndefinedOffsetMode the user has not requested for any specific offset.
	UndefinedOffsetMode OffsetMode = iota
	// MillisecondsOffsetMode the closet available offset at a given time will be fetched from the server
	// before the consumer starts pulling messages from Kafka.
	MillisecondsOffsetMode
	// ExplicitOffsetMode the user has explicitly asked for a specific offset.
	ExplicitOffsetMode
)

// Checkpoint represents a point in time or offset, from which the consumer has to start consuming from the specified topic.
type Checkpoint struct {
	offset int64
	at     time.Time
	mode   OffsetMode
}

// NewCheckpoint creates a new checkpoint instance.
//
// In rewind mode, the consumer will start consuming from the oldest available offset which means to consume all the old
// messages from the beginning of the stream.
func NewCheckpoint(rewind bool) *Checkpoint {
	offset := sarama.OffsetNewest
	if rewind {
		offset = sarama.OffsetOldest
	}
	return &Checkpoint{
		offset: offset,
		mode:   UndefinedOffsetMode,
	}
}

// Offset returns the final offset value from which consuming will be started.
//
// In MillisecondsOffsetMode, the offset will be the milliseconds of the specified time.
// This is what Kafka needs to figure out the closest available offset at the given time.
func (c *Checkpoint) Offset() int64 {
	return c.offset
}

// SetOffset sets the offset of the checkpoint and switches the mode to ExplicitOffsetMode.
func (c *Checkpoint) SetOffset(offset int64) {
	if offset < -2 {
		offset = -2
	}
	c.offset = offset
	c.mode = ExplicitOffsetMode
}

// TimeOffset returns the originally provided time value of the time-based offset in MillisecondsOffsetMode mode.
func (c *Checkpoint) TimeOffset() time.Time {
	return c.at
}

// SetTimeOffset sets the offset to the milliseconds of the given time and sets the mode to MillisecondsOffsetMode.
func (c *Checkpoint) SetTimeOffset(at time.Time) {
	var offsetMilliSeconds int64
	switch {
	case at.IsZero():
		offsetMilliSeconds = sarama.OffsetOldest
	default:
		offsetMilliSeconds = at.UnixNano() / 1000000
	}
	c.offset = offsetMilliSeconds
	c.mode = MillisecondsOffsetMode
	c.at = at
}

// OffsetString returns the string representation of the time offset in `02-01-2006T15:04:05.999999999` format if in
// MillisecondsOffsetMode mode, otherwise returns the string representation of the offset value.
func (c *Checkpoint) OffsetString() string {
	if c.mode == MillisecondsOffsetMode {
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

// Mode returns the current mode of the checkpoint.
func (c *Checkpoint) Mode() OffsetMode {
	if c == nil {
		return UndefinedOffsetMode
	}
	return c.mode
}
