package internal

import "strings"

func IsEmpty(val string) bool {
	return len(strings.TrimSpace(val)) == 0
}
