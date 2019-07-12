package internal

import (
	"math"
	"testing"
)

func TestToVerbosityLevel(t *testing.T) {
	testCases := []struct {
		title    string
		counter  int
		expected VerbosityLevel
	}{
		{
			title:    "negative value",
			counter:  -1,
			expected: Quiet,
		},
		{
			title:    "zero value",
			counter:  0,
			expected: Quiet,
		},
		{
			title:    "one",
			counter:  1,
			expected: Normal,
		},
		{
			title:    "two",
			counter:  2,
			expected: Verbose,
		},
		{
			title:    "three",
			counter:  3,
			expected: SuperVerbose,
		},
		{
			title:    "greater than three",
			counter:  4,
			expected: SuperVerbose,
		},
		{
			title:    "max int",
			counter:  int(math.MaxInt64),
			expected: SuperVerbose,
		},
	}

	for _, tc := range testCases {
		t.Run(tc.title, func(t *testing.T) {
			actual := ToVerbosityLevel(tc.counter)
			if actual != tc.expected {
				t.Errorf("Expected verbosity level: %d, Actual: %d", tc.expected, actual)
			}
		})
	}
}
