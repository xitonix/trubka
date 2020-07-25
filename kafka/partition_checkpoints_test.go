package kafka

import (
	"errors"
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/araddon/dateparse"
)

func TestPartitionCheckpointsGet(t *testing.T) {
	fromTimeValue := "2019-01-01T12:00:00+10:00"
	fromTimestamp, _ := dateparse.ParseAny(fromTimeValue)
	toTimeValue := "2020-01-01T12:00:00+10:00"
	toTimestamp, _ := dateparse.ParseAny(toTimeValue)
	randomPartition := randomInt32(3, 900)
	testCases := []struct {
		title          string
		from           []string
		to             []string
		expected       map[int32]*checkpointPair
		exclusive      bool
		expectedError  string
		expectedLength int
	}{
		{
			title: "empty start and stop checkpoints in inclusive mode",
			expected: map[int32]*checkpointPair{
				randomPartition: {
					from: newPredefinedCheckpoint(false),
				}},
			expectedLength: 1,
		},
		{
			title: "empty start and stop checkpoints in exclusive mode",
			expected: map[int32]*checkpointPair{
				randomPartition: {
					from: newPredefinedCheckpoint(false),
				}},
			expectedLength: 1,
			exclusive:      true,
		},
		{
			title: "the last global start checkpoint wins",
			from:  []string{"newest", "oldest", "100"},
			expected: map[int32]*checkpointPair{
				randomPartition: {
					from: newExplicitCheckpoint(100),
				}},
			expectedLength: 1,
		},
		{
			title: "the last global stop checkpoint wins",
			to:    []string{"200", "300", "100"},
			expected: map[int32]*checkpointPair{
				randomPartition: {
					from: newPredefinedCheckpoint(false),
					to:   newExplicitCheckpoint(100),
				}},
			expectedLength: 1,
		},
		{
			title: "stop checkpoints are parsed after start checkpoints",
			from:  []string{"100"},
			to:    []string{"200"},
			expected: map[int32]*checkpointPair{
				randomPartition: {
					from: newExplicitCheckpoint(100),
					to:   newExplicitCheckpoint(200),
				}},
			expectedLength: 1,
		},
		{
			title: "explicit start checkpoint with no global condition in inclusive mode",
			from:  []string{"1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(100),
				},
				randomPartition: {
					from: newPredefinedCheckpoint(false),
				},
			},
			expectedLength: 2,
		},
		{
			title: "explicit start checkpoint with no global condition in exclusive mode",
			from:  []string{"1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(100),
				},
				randomPartition: nil,
			},
			expectedLength: 2,
			exclusive:      true,
		},
		{
			title: "explicit start checkpoint with global condition in inclusive mode",
			from:  []string{"local", "1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(100),
				},
				randomPartition: {
					from: newLocalCheckpoint(),
				},
			},
			expectedLength: 2,
		},
		{
			title: "explicit start checkpoint with global condition in exclusive mode",
			from:  []string{"local", "1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(100),
				},
				randomPartition: nil,
			},
			expectedLength: 2,
			exclusive:      true,
		},
		{
			title: "explicit stop checkpoint with no global condition in inclusive mode",
			to:    []string{"1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newPredefinedCheckpoint(false),
					to:   newExplicitCheckpoint(100),
				},
				randomPartition: {
					from: newPredefinedCheckpoint(false),
				},
			},
			expectedLength: 2,
		},
		{
			title: "explicit stop checkpoint with no global condition in exclusive mode",
			to:    []string{"1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newPredefinedCheckpoint(false),
					to:   newExplicitCheckpoint(100),
				},
				randomPartition: nil,
			},
			expectedLength: 2,
			exclusive:      true,
		},
		{
			title: "explicit stop checkpoint with global condition in inclusive mode",
			to:    []string{"200", "1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newPredefinedCheckpoint(false),
					to:   newExplicitCheckpoint(100),
				},
				randomPartition: {
					from: newPredefinedCheckpoint(false),
					to:   newExplicitCheckpoint(200),
				},
			},
			expectedLength: 2,
		},
		{
			title: "explicit stop checkpoint with global condition in exclusive mode",
			to:    []string{"200", "1#100"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newPredefinedCheckpoint(false),
					to:   newExplicitCheckpoint(100),
				},
				randomPartition: nil,
			},
			expectedLength: 2,
			exclusive:      true,
		},
		{
			title: "explicit start and stop checkpoints in inclusive mode",
			from:  []string{"1#200"},
			to:    []string{"1#500"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(200),
					to:   newExplicitCheckpoint(500),
				},
				randomPartition: {
					from: newPredefinedCheckpoint(false),
				},
			},
			expectedLength: 2,
		},
		{
			title: "explicit start and stop checkpoints in exclusive mode",
			from:  []string{"1#200"},
			to:    []string{"1#500"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(200),
					to:   newExplicitCheckpoint(500),
				},
				randomPartition: nil,
			},
			expectedLength: 2,
			exclusive:      true,
		},
		{
			title: "explicit start and stop timestamp checkpoints in inclusive mode",
			from:  []string{"1#" + fromTimeValue},
			to:    []string{"1#" + toTimeValue},
			expected: map[int32]*checkpointPair{
				1: {
					from: newTimeCheckpoint(fromTimestamp),
					to:   newTimeCheckpoint(toTimestamp),
				},
				randomPartition: {
					from: newPredefinedCheckpoint(false),
				},
			},
			expectedLength: 2,
		},
		{
			title: "explicit start and stop timestamp checkpoints in exclusive mode",
			from:  []string{"1#" + fromTimeValue},
			to:    []string{"1#" + toTimeValue},
			expected: map[int32]*checkpointPair{
				1: {
					from: newTimeCheckpoint(fromTimestamp),
					to:   newTimeCheckpoint(toTimestamp),
				},
				randomPartition: nil,
			},
			expectedLength: 2,
			exclusive:      true,
		},
		{
			title: "global start and stop timestamp checkpoints in inclusive mode",
			from:  []string{fromTimeValue},
			to:    []string{toTimeValue},
			expected: map[int32]*checkpointPair{
				1: {
					from: newTimeCheckpoint(fromTimestamp),
					to:   newTimeCheckpoint(toTimestamp),
				},
				randomPartition: {
					from: newTimeCheckpoint(fromTimestamp),
					to:   newTimeCheckpoint(toTimestamp),
				},
			},
			expectedLength: 1,
		},
		{
			title: "global start and stop timestamp checkpoints in exclusive mode",
			from:  []string{fromTimeValue},
			to:    []string{toTimeValue},
			expected: map[int32]*checkpointPair{
				1: {
					from: newTimeCheckpoint(fromTimestamp),
					to:   newTimeCheckpoint(toTimestamp),
				},
				randomPartition: {
					from: newTimeCheckpoint(fromTimestamp),
					to:   newTimeCheckpoint(toTimestamp),
				},
			},
			expectedLength: 1,
			exclusive:      true,
		},
		{
			title: "global stop checkpoints apply to all the partitions with explicit start checkpoints if it has not been explicitly defined",
			from:  []string{"50", "1#100", "2#80"},
			to:    []string{"90", "1#150"},
			expected: map[int32]*checkpointPair{
				1: {
					from: newExplicitCheckpoint(100),
					to:   newExplicitCheckpoint(150),
				},
				2: {
					from: newExplicitCheckpoint(80),
					to:   newExplicitCheckpoint(90),
				},
			},
			expectedLength: 3,
		},
		{
			title:         "out of order explicit start and stop checkpoints",
			from:          []string{"1#500"},
			to:            []string{"1#200"},
			expectedError: "must be before stop offset",
		},
		{
			title:         "out of order global start and stop checkpoints",
			from:          []string{"500"},
			to:            []string{"200"},
			expectedError: "must be before stop offset",
		},
		{
			title:         "out of order global start and explicit stop checkpoints",
			from:          []string{"500"},
			to:            []string{"1#200"},
			expectedError: "must be before stop offset",
		},
		{
			title:         "out of order start and stop timestamp checkpoints",
			from:          []string{"1#" + toTimeValue},
			to:            []string{"1#" + fromTimeValue},
			expectedError: "must be before stop offset",
		},
		{
			title:         "explicit start checkpoint with invalid partition value",
			from:          []string{"invalid#100"},
			expectedError: "invalid partition value",
		},
		{
			title:         "explicit start checkpoint with invalid offset value",
			from:          []string{"1#invalid"},
			expectedError: "invalid offset value",
		},
		{
			title:         "global start checkpoint with invalid offset value",
			from:          []string{"invalid"},
			expectedError: "invalid offset value",
		},
		{
			title:         "explicit stop checkpoint with invalid partition value",
			to:            []string{"invalid#100"},
			expectedError: "invalid partition value",
		},
		{
			title:         "explicit stop checkpoint with invalid offset value",
			to:            []string{"1#invalid"},
			expectedError: "invalid offset value",
		},
		{
			title:         "global stop checkpoint with invalid offset value",
			to:            []string{"invalid"},
			expectedError: "invalid offset value",
		},
		{
			title:         "explicit start checkpoint with negative partition value",
			from:          []string{"-1#100"},
			expectedError: "partition cannot be a negative value",
		},
		{
			title:         "explicit start checkpoint with negative offset value",
			from:          []string{"1#-1"},
			expectedError: "offset cannot be a negative value",
		},
		{
			title:         "explicit stop checkpoint with negative partition value",
			to:            []string{"-1#100"},
			expectedError: "partition cannot be a negative value",
		},
		{
			title:         "explicit stop checkpoint with negative offset value",
			to:            []string{"1#-1"},
			expectedError: "offset cannot be a negative value",
		},
		{
			title:         "malformed explicit start offset with a single delimiter",
			from:          []string{"#"},
			expectedError: "invalid partition value",
		},
		{
			title:         "malformed explicit start offset with white space around the delimiter",
			from:          []string{" # "},
			expectedError: "invalid partition value",
		},
		{
			title:         "malformed explicit start offset with more than one delimiter sign",
			from:          []string{"1#10#100"},
			expectedError: "invalid start/stop value",
		},
		{
			title:         "malformed explicit stop offset with a single delimiter",
			to:            []string{"#"},
			expectedError: "invalid partition value",
		},
		{
			title:         "malformed explicit stop offset with white space around the delimiter",
			to:            []string{" # "},
			expectedError: "invalid partition value",
		},
		{
			title:         "malformed explicit stop offset with more than one delimiter sign",
			to:            []string{"1#10#100"},
			expectedError: "invalid start/stop value",
		},
	}
	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			checkpoints, err := NewPartitionCheckpoints(tC.from, tC.to, tC.exclusive)
			if !checkError(err, tC.expectedError) {
				t.Errorf("Expected error: %q, Actual: %s", tC.expectedError, err)
			}
			if tC.expectedError != "" {
				return
			}
			if tC.expectedLength != len(checkpoints.partitionCheckpoints) {
				t.Errorf("Expected Number of Checkpoint Pairs: %v, Actual: %v", tC.expectedLength, len(checkpoints.partitionCheckpoints))
			}
			for partition, expectedPair := range tC.expected {
				actual := checkpoints.get(partition)
				if err := comparePairs(expectedPair, actual); err != nil {
					t.Errorf("%s", err)
				}
			}
		})
	}
}

func comparePairs(expected, actual *checkpointPair) error {
	if actual == nil {
		if expected != nil {
			return errors.New("actual checkpoint pair was nil but the expected pair was not")
		}
		return nil
	}
	if expected == nil {
		return errors.New("actual checkpoint pair was not nil but the expected pair was")
	}

	if err := compareCheckpoints("From", expected.from, actual.from); err != nil {
		return err
	}

	return compareCheckpoints("To", expected.to, actual.to)
}

func compareCheckpoints(name string, expected, actual *checkpoint) error {
	if actual == nil {
		if expected != nil {
			return fmt.Errorf("%s checkpoint was expected to be nil, but it wasn't", name)
		}
		return nil
	}
	if expected == nil {
		return fmt.Errorf("%s checkpoint was not expected to be nil, but it was", name)
	}
	if actual.at.Equal(expected.at) && actual.offset == expected.offset && actual.mode == expected.mode {
		return nil
	}

	return fmt.Errorf("expected %s checkpoint: %v|%v|%v, actual: %v|%v|%v",
		name,
		modeToString(expected.mode),
		getOffsetString(expected.offset),
		expected.at,
		modeToString(actual.mode),
		getOffsetString(actual.offset),
		actual.at)
}

func checkError(actual error, expected string) bool {
	if actual == nil {
		return expected == ""
	}
	if expected == "" {
		return false
	}
	return strings.Contains(actual.Error(), expected)
}

func modeToString(mode checkpointMode) string {
	switch mode {
	case predefinedMode:
		return "predefined"
	case explicitMode:
		return "explicit"
	case localMode:
		return "local"
	case timestampMode:
		return "timestamp"
	default:
		return "unknown"
	}
}

func randomInt32(min, max int32) int32 {
	rand.Seed(time.Now().UnixNano())
	return rand.Int31n(max-min+1) + min
}
