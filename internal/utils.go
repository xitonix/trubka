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

// FormatTimeForHuman formats the time using `02 Jan 2006 15:04:05.999999999` layout.
func FormatTimeForHuman(t time.Time) string {
	return t.Format("02 Jan 2006 15:04:05.999999999")
}

// FormatTimeForMachine formats the time using `2006-01-02T15:04:05.999999999` layout.
func FormatTimeForMachine(t time.Time) string {
	// yyyy-mm-dd
	return t.Format("2006-01-02T15:04:05.999999999")
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
	signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
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
