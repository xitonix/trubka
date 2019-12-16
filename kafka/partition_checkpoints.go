package kafka

import (
	"fmt"
	"strconv"
	"strings"
	"time"
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
)

type PartitionCheckpoints struct {
	mode                 OffsetMode
	partitionCheckpoints map[int32]*Checkpoint
}

func NewPartitionCheckpoints(rewind bool) *PartitionCheckpoints {
	return &PartitionCheckpoints{
		mode: UndefinedOffsetMode,
		partitionCheckpoints: map[int32]*Checkpoint{
			allPartitions: newCheckpoint(rewind),
		}}
}

func (p *PartitionCheckpoints) SetToTimeCheckpoint(t time.Time) {
	p.mode = MillisecondsOffsetMode
	p.partitionCheckpoints[allPartitions] = newTimeCheckpoint(t)
}

func (p *PartitionCheckpoints) Add(pcp string) error {
	parts := strings.Split(pcp, ":")
	if len(parts) != 2 {
		return fmt.Errorf("invalid partition offset string. Make sure you follow partition:offset format")
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
	}

	if offsetStr != "" {
		offset, err := strconv.ParseInt(offsetStr, 10, 64)
		if err != nil {
			return fmt.Errorf("invalid offset value: %w", err)
		}
		if offset >= 0 {
			p.mode = ExplicitOffsetMode
			p.partitionCheckpoints[partition] = newOffsetCheckpoint(offset)
		}
	}
	return nil
}

func (p *PartitionCheckpoints) GetDefault() *Checkpoint {
	return p.partitionCheckpoints[allPartitions]
}

func (p *PartitionCheckpoints) GetExplicitOffsets() string {
	offsets := make([]string, 0)
	for partition, cp := range p.partitionCheckpoints {
		if partition != allPartitions {
			offsets = append(offsets, fmt.Sprintf("%d:%s", partition, cp.OffsetString()))
		}
	}
	return strings.Join(offsets, ",")
}

func (p *PartitionCheckpoints) Get(partition int32) *Checkpoint {
	return p.partitionCheckpoints[partition]
}

// Mode returns the current check-pointing mode.
func (p *PartitionCheckpoints) Mode() OffsetMode {
	if p == nil {
		return UndefinedOffsetMode
	}
	return p.mode
}
