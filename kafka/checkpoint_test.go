package kafka

import (
	"testing"

	"github.com/araddon/dateparse"
)

func TestParseCheckpoint(t *testing.T) {
	timeValue := "2020-07-15T14:24:23.703+10:00"
	timestamp, _ := dateparse.ParseAny(timeValue)
	testCases := []struct {
		title            string
		raw              string
		expected         *checkpoint
		expectError      string
		isStopCheckpoint bool
	}{
		{
			title:       "empty input",
			expectError: "checkpoint value cannot be empty",
		},
		{
			title:    "oldest",
			raw:      "oldest",
			expected: newPredefinedCheckpoint(true),
		},
		{
			title:    "beginning",
			raw:      "beginning",
			expected: newPredefinedCheckpoint(true),
		},
		{
			title:    "earliest",
			raw:      "earliest",
			expected: newPredefinedCheckpoint(true),
		},
		{
			title:    "start",
			raw:      "start",
			expected: newPredefinedCheckpoint(true),
		},
		{
			title:    "newest",
			raw:      "newest",
			expected: newPredefinedCheckpoint(false),
		},
		{
			title:    "latest",
			raw:      "latest",
			expected: newPredefinedCheckpoint(false),
		},
		{
			title:    "end",
			raw:      "end",
			expected: newPredefinedCheckpoint(false),
		},
		{
			title:    "local",
			raw:      "local",
			expected: newLocalCheckpoint(),
		},
		{
			title:    "stored",
			raw:      "stored",
			expected: newLocalCheckpoint(),
		},
		{
			title:    "time based",
			raw:      timeValue,
			expected: newTimeCheckpoint(timestamp),
		},
		{
			title:    "explicit",
			raw:      "100",
			expected: newExplicitCheckpoint(100),
		},
		{
			title:       "invalid value",
			raw:         "invalid",
			expectError: "invalid offset value",
		},
		{
			title:       "empty input for stop offset",
			expectError: "checkpoint value cannot be empty",
		},
		{
			title:            "oldest stop offset",
			raw:              "oldest",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "beginning stop offset",
			raw:              "beginning",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "earliest stop offset",
			raw:              "earliest",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "start stop offset",
			raw:              "start",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "newest stop offset",
			raw:              "newest",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "latest stop offset",
			raw:              "latest",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "end stop offset",
			raw:              "end",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "local stop offset",
			raw:              "local",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:            "stored stop offset",
			raw:              "stored",
			expectError:      "is not an acceptable stop condition",
			isStopCheckpoint: true,
		},
		{
			title:    "time based stop offset",
			raw:      timeValue,
			expected: newTimeCheckpoint(timestamp),
		},
		{
			title:    "explicit stop offset",
			raw:      "100",
			expected: newExplicitCheckpoint(100),
		},
		{
			title:            "invalid value",
			raw:              "invalid",
			expectError:      "invalid offset value",
			isStopCheckpoint: true,
		},
	}
	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			actual, err := parseCheckpoint(tC.raw, tC.isStopCheckpoint)
			if !checkError(err, tC.expectError) {
				t.Errorf("Expected error: %q, Actual: %s", tC.expectError, err)
			}
			if err := compareCheckpoints("parsed", actual, tC.expected); err != nil {
				t.Errorf("%s", err)
			}
		})
	}
}
