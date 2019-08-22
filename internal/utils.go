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
	return t.Format("02-01-2006T15:04:05.999999999")
}

func FormatTimeUTC(t time.Time) string {
	return FormatTime(t) + " UTC"
}

func BoolToString(in bool) string {
	if in {
		return "Yes"
	}
	return "No"
}
