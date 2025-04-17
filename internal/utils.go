package internal

import (
	"errors"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"
)

// IsEmpty returns true of the trimmed input is empty.
func IsEmpty(val string) bool {
	return len(strings.TrimSpace(val)) == 0
}

// FormatTime formats the time using time.RFC3339Nano layout.
func FormatTime(t time.Time) string {
	return t.Format(time.RFC3339Nano)
}

// NotFoundError constructs a new NotFound error for the specified entity.
func NotFoundError(entity, filterName string, ex *regexp.Regexp) error {
	msg := fmt.Sprintf("No %s has been found.", entity)
	if ex != nil {
		msg += fmt.Sprintf(" You might need to tweak the %s filter (%s).", filterName, ex.String())
	}
	return errors.New(msg)
}

// WaitForCancellationSignal waits for the user to press Ctrl+C.
func WaitForCancellationSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	<-signals
}

// RemovePort removes :Port from an address if exists.
func RemovePort(address string) string {
	if i := strings.Index(address, ":"); i > 0 {
		return address[:i]
	}
	return address
}

// IgnoreRegexCase returns a case-insensitive regular expression.
func IgnoreRegexCase(r *regexp.Regexp) (*regexp.Regexp, error) {
	if r == nil {
		return r, nil
	}
	ex, err := regexp.Compile("(?i)" + r.String())
	if err != nil {
		return nil, err
	}
	return ex, nil
}

// Title capitalises the first letter of the error message if the error is not nil, otherwise returns "".
func Title(err error) string {
	if err == nil {
		return ""
	}
	input := err.Error()
	if input == "" {
		return ""
	}
	r, n := utf8.DecodeRuneInString(input)
	return string(unicode.ToUpper(r)) + input[n:]
}
