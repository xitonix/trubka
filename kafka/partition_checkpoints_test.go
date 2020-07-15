package kafka

import (
	"errors"
	"fmt"
	"strings"
	"testing"
)

func TestNewPartitionCheckpoints(t *testing.T) {
	testCases := []struct {
		title              string
		from               []string
		to                 []string
		expected           map[int32]*checkpointPair
		expectedApplyToAll bool
		expectError        string
		expectedLength     int
	}{
		{
			title: "empty start and stop checkpoints",
			expected: map[int32]*checkpointPair{
				allPartitions: {
					from: newPredefinedCheckpoint(false),
				}},
			expectedLength: 1,
		},
		{
			title: "the last global checkpoint wins",
			from:  []string{"newest", "oldest", "100"},
			expected: map[int32]*checkpointPair{
				allPartitions: {
					from: newExplicitCheckpoint(100),
				}},
			expectedLength: 1,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			checkpoints, err := NewPartitionCheckpoints(tC.from, tC.to)
			if !checkError(err, tC.expectError) {
				t.Errorf("Expected error: %q, Actual: %s", tC.expectError, err)
			}
			if tC.expectedApplyToAll != checkpoints.applyToAll {
				t.Errorf("Expected Apply to All: %v, Actual: %v", tC.expectedApplyToAll, checkpoints.applyToAll)
			}
			if tC.expectedLength != len(checkpoints.partitionCheckpoints) {
				t.Errorf("Expected Number of Checkpoint Pairs: %v, Actual: %v", tC.expectedLength, len(checkpoints.partitionCheckpoints))
			}
			for partition, actualPair := range checkpoints.partitionCheckpoints {
				expectedPair, ok := tC.expected[partition]
				if !ok {
					t.Errorf("Checkpoint pair for partition %d not found", partition)
				}
				if err := comparePairs(actualPair, expectedPair); err != nil {
					t.Errorf("%s", err)
				}
			}
		})
	}
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

func comparePairs(actual, expected *checkpointPair) error {
	if actual == nil {
		if expected != nil {
			return errors.New("expected checkpoint pair was not nil")
		}
		return nil
	}
	if expected == nil {
		return errors.New("expected checkpoint pair was nil")
	}

	if err := compareCheckpoints("From", actual.from, expected.from); err != nil {
		return err
	}

	return compareCheckpoints("To", actual.to, expected.to)
}

func compareCheckpoints(name string, actual, expected *checkpoint) error {
	if actual == nil {
		if expected != nil {
			return fmt.Errorf("%s checkpoint was expected to be nil, but it wasn't", name)
		}
		return nil
	}
	if expected == nil {
		return fmt.Errorf("%s checkpoint was not expected to be nil, but it was", name)
	}
	if actual.at == expected.at && actual.offset == expected.offset && actual.mode == expected.mode {
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
