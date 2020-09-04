package kafka

import (
	"bytes"
	"encoding/gob"
	"fmt"
	"sort"
	"strings"
)

// PartitionOffset represents a map of partition offset pairs.
type PartitionOffset map[int32]Offset

// ToJSON returns an object ready to be serialised into json string.
func (p PartitionOffset) ToJSON() interface{} {
	if p == nil {
		return nil
	}
	type offset struct {
		Current int64 `json:"current_offset"`
		Latest  int64 `json:"latest_offset"`
		Lag     int64 `json:"lag"`
	}
	output := make(map[int32]offset, len(p))
	for partition, off := range p {
		output[partition] = offset{
			Current: off.Current,
			Latest:  off.Latest,
			Lag:     off.Lag(),
		}
	}
	return output
}

// SortPartitions returns a list of sorted partitions.
func (p PartitionOffset) SortPartitions() []int {
	sorted := make([]int, 0)
	if len(p) == 0 {
		return sorted
	}
	for partition := range p {
		sorted = append(sorted, int(partition))
	}
	sort.Ints(sorted)
	return sorted
}

// serialises the offset map and returns the bytes as well as the checksum string of the current values.
func (p PartitionOffset) marshal() (string, []byte, error) {
	if len(p) == 0 {
		return "", []byte{}, nil
	}
	toWrite := make(map[int32]int64)
	for pt, of := range p {
		if of.Current >= 0 {
			toWrite[pt] = of.Current
		}
	}
	if len(toWrite) == 0 {
		return "", nil, nil
	}
	buff := bytes.Buffer{}
	enc := gob.NewEncoder(&buff)
	err := enc.Encode(toWrite)
	if err != nil {
		return "", nil, err
	}
	return strings.Replace(fmt.Sprintf("%v", toWrite), "map", "", 1), buff.Bytes(), nil
}

func (p PartitionOffset) copyTo(dest PartitionOffset) {
	if len(p) == 0 {
		return
	}
	if dest == nil {
		dest = make(PartitionOffset)
	}
	for partition, offset := range p {
		if offset.Current >= 0 {
			dest[partition] = offset
		}
	}
}

// ToPartitionOffset creates a new PartitionOffset map from a raw map.
//
// Set latest parameter to true, if you would like to set the Latest offset value instead of the Current value.
func ToPartitionOffset(po map[int32]int64, latest bool) PartitionOffset {
	result := make(PartitionOffset)
	for partition, offset := range po {
		off := Offset{}
		if latest {
			off.Latest = offset
		} else {
			off.Current = offset
		}
		result[partition] = off
	}
	return result
}
