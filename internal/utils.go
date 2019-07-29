package internal

import "strings"

// IsEmpty returns true of the trimmed input is empty.
func IsEmpty(val string) bool {
	return len(strings.TrimSpace(val)) == 0
}
