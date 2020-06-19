package internal

import (
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/pkg/errors"
)

// IsEmpty returns true of the trimmed input is empty.
func IsEmpty(val string) bool {
	return len(strings.TrimSpace(val)) == 0
}

func FormatTime(t time.Time) string {
	return t.Format("02-01-2006T15:04:05.999999999")
}

func NotFoundError(entity, filterName string, ex *regexp.Regexp) error {
	msg := fmt.Sprintf("No %s has been found.", entity)
	if ex != nil {
		msg += fmt.Sprintf(" You might need to tweak the %s filter (%s).", filterName, ex.String())
	}
	return errors.New(msg)
}

func FormatTimeUTC(t time.Time) string {
	return FormatTime(t) + " UTC"
}

func PrependTimestamp(ts time.Time, in []byte) []byte {
	return append([]byte(fmt.Sprintf("%s\n", FormatTimeUTC(ts))), in...)
}

func PrependTopic(topic string, in []byte) []byte {
	return append([]byte(fmt.Sprintf("%s\n", topic)), in...)
}

func PrependKey(key []byte, partition int32, in []byte) []byte {
	return append([]byte(fmt.Sprintf("P%d: %X\n", partition, key)), in...)
}

func WaitForCancellationSignal() {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
	<-signals
}

func RemovePort(address string) string {
	if i := strings.Index(address, ":"); i > 0 {
		return address[:i]
	}
	return address
}

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

func BoolToString(b bool) string {
	if b {
		return "YES"
	}
	return "NO"
}

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
