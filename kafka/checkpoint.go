package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

type offsetMode int8

const (
	UndefinedOffsetMode offsetMode = iota
	MillisecondsOffsetMode
	ExplicitOffsetMode
)

type Checkpoint struct {
	offset int64
	at     time.Time
	mode   offsetMode
}

func (c *Checkpoint) Offset() int64 {
	return c.offset
}

func (c *Checkpoint) SetOffset(offset int64) {
	if offset < -2 {
		offset = -2
	}
	c.offset = offset
	c.mode = ExplicitOffsetMode
}

func (c *Checkpoint) TimeOffset() time.Time {
	return c.at
}

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

func (c *Checkpoint) Mode() offsetMode {
	if c == nil {
		return UndefinedOffsetMode
	}
	return c.mode
}

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
