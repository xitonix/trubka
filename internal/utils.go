package internal

import (
	"strings"
	"time"
)

// IsEmpty returns true of the trimmed input is empty.
func IsEmpty(val string) bool {
	return len(strings.TrimSpace(val)) == 0
}

func FormatTime(t time.Time) string {
	return t.Format("02-01-2006T15:04:05.999999999") + " UTC"
}
