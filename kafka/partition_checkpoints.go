package kafka

import (
	"fmt"
	"strconv"
	"strings"

	"github.com/araddon/dateparse"
)

const (
	allPartitions    int32 = -1
	invalidPartition int32 = -2
)

// OffsetMode represents the offset mode for a checkpoint.
type OffsetMode int8

const (
	// UndefinedOffsetMode parsing an explicit offset value has failed.
	UndefinedOffsetMode OffsetMode = iota
	// PredefinedOffsetMode the user picked one of the predefined Sarama values (eg. newest, oldest etc)
	PredefinedOffsetMode
	// ExplicitOffsetMode the user has explicitly asked for a specific offset.
	ExplicitOffsetMode
	// LocalOffsetMode the offset needs to be read from the local offset store.
	LocalOffsetMode
)

// PartitionCheckpoints holds a list of explicitly requested offsets (if applicable).
//
// If no partition offset has been explicitly asked by the user, the time-based or newest/oldest offset
// will be stored as the only value in the map under `allPartitions` key.
type PartitionCheckpoints struct {
	mode                 OffsetMode
	partitionCheckpoints map[int32]*CheckpointPair
	from                 string
	to                   string
}

// NewPartitionCheckpoints creates a new instance of partition checkpoints.
func NewPartitionCheckpoints(from, to []string) (*PartitionCheckpoints, error) {
	var (
		err         error
		mode        = PredefinedOffsetMode
		checkpoints = map[int32]*CheckpointPair{
			allPartitions: {
				From: newCheckpoint(false),
			},
		}
	)

	for _, raw := range from {
		mode, err = parseFrom(raw, checkpoints)
		if err != nil {
			return nil, err
		}
	}

	for _, raw := range to {
		err = parseTo(raw, checkpoints)
		if err != nil {
			return nil, err
		}
	}

	for _, cp := range checkpoints {
		if cp.From.after(cp.To) {
			return nil, fmt.Errorf("start offset '%v' must be before stop offset '%v'", cp.From.OffsetString(), cp.To.OffsetString())
		}
	}

	return &PartitionCheckpoints{
		mode:                 mode,
		partitionCheckpoints: checkpoints,
		from:                 strings.Join(from, ","),
		to:                   strings.Join(to, ","),
	}, nil
}

// get returns the offset explicitly asked by the user.
//
// If the partition offset is not found in the list of explicitly requested offsets, this method
// will return true if the offset needs to be applied to all the other partitions (this is to cover --from-offset :Offset)
func (p *PartitionCheckpoints) get(partition int32) *CheckpointPair {
	if pair, ok := p.partitionCheckpoints[partition]; ok {
		return pair
	}

	return p.partitionCheckpoints[allPartitions]
}

func parseTo(raw string, checkpoints map[int32]*CheckpointPair) error {
	cp, partition, err := parseExplicit(raw)
	if err != nil {
		return err
	}
	if pair, ok := checkpoints[partition]; ok {
		pair.To = cp
		return nil
	}
	checkpoints[partition] = &CheckpointPair{
		From: newCheckpoint(true),
		To:   cp,
	}
	return nil
}

func parseFrom(raw string, checkpoints map[int32]*CheckpointPair) (OffsetMode, error) {
	var mode OffsetMode
	switch strings.ToLower(raw) {
	case "local", "stored":
		mode = LocalOffsetMode
		checkpoints[allPartitions] = &CheckpointPair{
			From: newCheckpoint(false),
		}
	case "newest", "latest", "end":
		mode = PredefinedOffsetMode
		checkpoints[allPartitions] = &CheckpointPair{
			From: newCheckpoint(false),
		}
	case "oldest", "earliest", "beginning", "start":
		mode = PredefinedOffsetMode
		checkpoints[allPartitions] = &CheckpointPair{
			From: newCheckpoint(true),
		}
	default:
		cp, partition, err := parseExplicit(raw)
		if err != nil {
			return UndefinedOffsetMode, err
		}
		mode = ExplicitOffsetMode
		checkpoints[partition] = &CheckpointPair{
			From: cp,
		}
	}
	return mode, nil
}

func parseExplicit(raw string) (*Checkpoint, int32, error) {
	t, err := dateparse.ParseAny(raw)
	if err == nil {
		return newTimeCheckpoint(t), allPartitions, nil
	}

	parts := strings.Split(raw, ":")
	if len(parts) != 2 {
		return nil, invalidPartition, fmt.Errorf("invalid partition offset string: %s", raw)
	}

	offsetStr := strings.TrimSpace(parts[1])

	offset, err := parseInt(offsetStr, "offset")
	if err != nil {
		return nil, invalidPartition, nil
	}

	cp := newOffsetCheckpoint(offset)

	partitionStr := strings.TrimSpace(parts[0])
	if partitionStr != "" {
		partition, err := parseInt(partitionStr, "partition")
		if err != nil {
			return nil, invalidPartition, nil
		}
		return cp, int32(partition), nil
	}

	// ":N" parameter has been provided. We need to apply the last winner offset (N)
	// value to all the partitions which is not explicitly requested.
	return cp, allPartitions, nil
}

func parseInt(value string, entity string) (int64, error) {
	parsed, err := strconv.ParseInt(value, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("invalid %s value: %w", entity, err)
	}
	if parsed < 0 {
		return 0, fmt.Errorf("%s cannot be a negative value", entity)
	}
	return parsed, nil
}
