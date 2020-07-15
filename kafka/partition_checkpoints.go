package kafka

import (
	"fmt"
	"strconv"
	"strings"
)

const (
	allPartitions         int32 = -1
	allPartitionsExplicit int32 = -2
	invalidPartition      int32 = -3
)

// PartitionCheckpoints holds a list of explicitly requested offsets (if applicable).
//
// If no partition offset has been explicitly asked by the user, the time-based or newest/oldest offset
// will be stored as the only value in the map under `allPartitions` key.
type PartitionCheckpoints struct {
	partitionCheckpoints map[int32]*checkpointPair
	applyToAll           bool
}

// NewPartitionCheckpoints creates a new instance of partition checkpoints.
func NewPartitionCheckpoints(from, to []string) (*PartitionCheckpoints, error) {
	var (
		checkpoints = map[int32]*checkpointPair{
			allPartitions: {
				from: newPredefinedCheckpoint(false),
			},
		}
	)

	var applyToAll bool
	for _, raw := range from {
		cp, partition, err := parse(raw, false)
		if err != nil {
			return nil, err
		}
		if partition == allPartitionsExplicit {
			applyToAll = true
			// We still need to store it under `allPartitions` key
			partition = allPartitions
		}
		checkpoints[partition] = &checkpointPair{
			from: cp,
		}
	}

	for _, raw := range to {
		cp, partition, err := parse(raw, true)
		if err != nil {
			return nil, err
		}
		if _, ok := checkpoints[partition]; !ok {
			checkpoints[partition] = &checkpointPair{
				from: checkpoints[allPartitions].from,
				to:   cp,
			}
		}
		checkpoints[partition].to = cp
	}

	for _, cp := range checkpoints {
		if cp.from.after(cp.to) {
			return nil, fmt.Errorf("start offset '%v' must be before stop offset '%v'", cp.from.String(), cp.to.String())
		}
	}

	return &PartitionCheckpoints{
		partitionCheckpoints: checkpoints,
		applyToAll:           applyToAll,
	}, nil
}

// get returns the checkpoint for the specified partition.
func (p *PartitionCheckpoints) get(partition int32) *checkpointPair {
	if pair, ok := p.partitionCheckpoints[partition]; ok {
		return pair
	}

	if !p.applyToAll && len(p.partitionCheckpoints) > 1 {
		// User explicitly asked for other partitions, but not this one.
		// We don't want to start consuming from this partition if it has not been asked by the user.
		return nil
	}

	return p.partitionCheckpoints[allPartitions]
}

func parse(raw string, isStopOffset bool) (*checkpoint, int32, error) {
	parts := strings.Split(raw, "#")
	switch len(parts) {
	case 1:
		cp, err := parseCheckpoint(raw, isStopOffset)
		if err != nil {
			return nil, invalidPartition, err
		}
		return cp, allPartitions, nil
	case 2:
		offset := strings.TrimSpace(parts[1])
		cp, err := parseCheckpoint(offset, isStopOffset)
		if err != nil {
			return nil, invalidPartition, err
		}
		partitionStr := strings.TrimSpace(parts[0])
		if partitionStr == "" {
			// ":N" parameter has been provided. We need to apply the last winner offset (N)
			// value to all the partitions which is not explicitly requested.
			return cp, allPartitionsExplicit, nil
		}
		partition, err := parseInt(partitionStr, "partition")
		if err != nil {
			return nil, invalidPartition, err
		}
		return cp, int32(partition), nil
	default:
		return nil, invalidPartition, fmt.Errorf("invalid partition offset string: %s", raw)
	}
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
