package kafka

import (
	"fmt"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/araddon/dateparse"

	"github.com/xitonix/trubka/internal"
)

type checkpointMode int8

const (
	// Predefined Sarama values (eg. newest, oldest etc)
	predefinedMode checkpointMode = iota
	explicitMode
	localMode
	timestampMode
)

type checkpointPair struct {
	from *checkpoint
	to   *checkpoint
}

type checkpoint struct {
	offset int64
	at     time.Time
	mode   checkpointMode
}

func newPredefinedCheckpoint(rewind bool) *checkpoint {
	offset := sarama.OffsetNewest
	if rewind {
		offset = sarama.OffsetOldest
	}
	return &checkpoint{
		offset: offset,
		mode:   predefinedMode,
	}
}

func newLocalCheckpoint() *checkpoint {
	return &checkpoint{
		mode: localMode,
	}
}

func newExplicitCheckpoint(offset int64) *checkpoint {
	if offset < sarama.OffsetOldest {
		offset = sarama.OffsetOldest
	}
	return &checkpoint{
		offset: offset,
		mode:   explicitMode,
	}
}

// newTimeCheckpoint creates a new checkpoint and sets the offset to the milliseconds of the given time and the mode to MillisecondsOffsetMode.
func newTimeCheckpoint(at time.Time) *checkpoint {
	var offsetMilliSeconds int64
	switch {
	case at.IsZero():
		offsetMilliSeconds = sarama.OffsetOldest
	default:
		offsetMilliSeconds = at.UnixNano() / 1000000
	}
	return &checkpoint{
		mode:   timestampMode,
		offset: offsetMilliSeconds,
		at:     at,
	}
}

func (c *checkpoint) String() string {
	if c.mode == timestampMode {
		return internal.FormatTime(c.at)
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

func (c *checkpoint) after(cp *checkpoint) bool {
	if c == nil || cp == nil {
		return false
	}

	if c.mode == timestampMode {
		if cp.mode == timestampMode {
			return c.at.After(cp.at)
		}
		return false
	}

	return c.offset > cp.offset
}

func parseCheckpoint(value string, isStopOffset bool) (*checkpoint, error) {
	if len(value) == 0 {
		return nil, fmt.Errorf("start/stop checkpoint value cannot be empty")
	}
	t, err := dateparse.ParseAny(value)
	if err == nil {
		return newTimeCheckpoint(t), nil
	}

	switch value {
	case "local", "stored":
		if isStopOffset {
			return nil, invalidToValue(value)
		}
		return newLocalCheckpoint(), nil
	case "newest", "latest", "end":
		if isStopOffset {
			return nil, invalidToValue(value)
		}
		return newPredefinedCheckpoint(false), nil
	case "oldest", "earliest", "beginning", "start":
		if isStopOffset {
			return nil, invalidToValue(value)
		}
		return newPredefinedCheckpoint(true), nil
	}

	offset, err := parseInt(value, "offset")
	if err != nil {
		return nil, err
	}

	return newExplicitCheckpoint(offset), nil
}

func invalidToValue(value string) error {
	return fmt.Errorf("'%s' is not an acceptable stop condition", value)
}
