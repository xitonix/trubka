package kafka

import (
	"time"

	"github.com/Shopify/sarama"
)

type offsetMode int8

const (
	undefined offsetMode = iota
	milliseconds
	explicit
)

type Checkpoint struct {
	offset int64
	at     time.Time
	mode   offsetMode
}

func NewTimeCheckpoint(at time.Time) *Checkpoint {
	var offsetMilliSeconds int64
	switch {
	case at.IsZero():
		offsetMilliSeconds = sarama.OffsetOldest
	default:
		offsetMilliSeconds = at.UnixNano() / 1000000
	}
	return &Checkpoint{
		offset: offsetMilliSeconds,
		mode:   milliseconds,
		at:     at,
	}
}

func NewOffsetCheckpoint(offset int64) *Checkpoint {
	if offset < -2 {
		offset = -2
	}
	return &Checkpoint{
		offset: offset,
		mode:   explicit,
	}
}

func NewCheckpoint(rewind bool) *Checkpoint {
	offset := sarama.OffsetNewest
	if rewind {
		offset = sarama.OffsetOldest
	}
	return &Checkpoint{
		offset: offset,
		mode:   undefined,
	}
}
