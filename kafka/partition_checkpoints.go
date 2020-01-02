package kafka

import (
	"errors"
	"fmt"
	"strconv"
	"strings"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

const (
	allPartitions int32 = -1
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
	// LocalOffsetMode the offset needs to be read from the local offset store.
	LocalOffsetMode
)

// PartitionCheckpoints holds a list of explicitly requested offsets (if applicable).
//
// If no partition offset has been explicitly asked by the user, the time-based or newest/oldest offset
// will be stored as the only value in the map under `allPartitions` key.
type PartitionCheckpoints struct {
	mode                     OffsetMode
	partitionCheckpoints     map[int32]*Checkpoint
	applyExplicitOffsetToAll bool
}

// NewPartitionCheckpoints creates a new instance of partition checkpoints.
func NewPartitionCheckpoints(from string) (*PartitionCheckpoints, error) {
	from = strings.TrimSpace(strings.ToLower(from))
	if from == "" {
		return nil, errors.New("the from offset cannot be empty")
	}

	switch from {
	case "local", "stored":
		return newPartitionCheckpoint(LocalOffsetMode, false), nil
	case "newest", "latest", "end":
		return newPartitionCheckpoint(UndefinedOffsetMode, false), nil
	case "oldest", "earliest", "beginning", "start":
		return newPartitionCheckpoint(UndefinedOffsetMode, true), nil
	default:
		return parseExplicitOffsets(from)
	}
}

func newPartitionCheckpoint(mode OffsetMode, rewind bool) *PartitionCheckpoints {
	return &PartitionCheckpoints{
		mode: mode,
		partitionCheckpoints: map[int32]*Checkpoint{
			allPartitions: newCheckpoint(rewind),
		}}
}

func parseExplicitOffsets(from string) (*PartitionCheckpoints, error) {

	t, err := internal.ParseTime(from)
	if err == nil {
		return &PartitionCheckpoints{
			mode: MillisecondsOffsetMode,
			partitionCheckpoints: map[int32]*Checkpoint{
				allPartitions: newTimeCheckpoint(t),
			}}, nil
	}

	parts := strings.Split(from, ",")
	checkpoints := newPartitionCheckpoint(UndefinedOffsetMode, false)
	for _, pcp := range parts {
		if err := checkpoints.add(pcp); err != nil {
			return nil, err
		}
	}

	return checkpoints, nil
}

func (p *PartitionCheckpoints) add(pcp string) error {
	parts := strings.Split(pcp, ":")
	if len(parts) != 2 {
		return errors.New("invalid partition offset string")
	}
	partitionStr := strings.TrimSpace(parts[0])
	offsetStr := strings.TrimSpace(parts[1])
	partition := allPartitions
	if partitionStr != "" {
		p, err := strconv.ParseInt(partitionStr, 10, 32)
		if err != nil {
			return fmt.Errorf("invalid partition value: %w", err)
		}
		if p < 0 {
			return fmt.Errorf("%d is not a valid partition value. Partition must be greater than zero", p)
		}
		partition = int32(p)
	} else {
		// ":N" parameter has been provided. We need to apply the last winner offset (N)
		// value to all the partitions which is not explicitly requested.
		p.applyExplicitOffsetToAll = true
	}

	offset := sarama.OffsetNewest
	if offsetStr != "" {
		o, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset value: %w", err)
		}
		if o < 0 {
			return fmt.Errorf("offset cannot be a negative value")
		}
		offset = o
	}

	p.mode = ExplicitOffsetMode
	p.partitionCheckpoints[partition] = newOffsetCheckpoint(offset)

	return nil
}

// GetDefault returns the default checkpoint applicable to all the partitions.
//
// This checkpoint must ONLY be used when no partition offset has been explicitly requested by the user.
func (p *PartitionCheckpoints) GetDefault() *Checkpoint {
	return p.partitionCheckpoints[allPartitions]
}

// GetExplicitOffsets returns the string representation of all the explicitly requested partition offsets.
func (p *PartitionCheckpoints) GetExplicitOffsets() string {
	if p.mode != ExplicitOffsetMode {
		return ""
	}
	offsets := make([]string, 0)
	for partition, cp := range p.partitionCheckpoints {
		if partition == allPartitions {
			if p.applyExplicitOffsetToAll {
				offsets = append(offsets, fmt.Sprintf(":%s", cp.OffsetString()))
			}
		} else {
			offsets = append(offsets, fmt.Sprintf("%d:%s", partition, cp.OffsetString()))
		}
	}
	return "[" + strings.Join(offsets, ",") + "]"
}

// Get returns the offset explicitly asked by the user.
//
// If the partition offset is not found in the list of explicitly requested offsets, this method
// will return true if the offset needs to be applied to all the other partitions (this is to cover --from-offset :Offset)
func (p *PartitionCheckpoints) Get(partition int32) (*Checkpoint, bool) {
	if cp, ok := p.partitionCheckpoints[partition]; ok {
		return cp, ok
	}

	return p.partitionCheckpoints[allPartitions], p.applyExplicitOffsetToAll
}

// Mode returns the current check-pointing mode.
func (p *PartitionCheckpoints) Mode() OffsetMode {
	if p == nil {
		return UndefinedOffsetMode
	}
	return p.mode
}
