package internal

import (
	"fmt"
	"log"
	"os"
	"time"
)

// Logger represents a logger.
type Logger struct {
	currentLevel VerbosityLevel
	logger       log.Logger
}

// NewLogger creates a new instance of a logger.
func NewLogger(level VerbosityLevel) *Logger {
	return &Logger{currentLevel: level, logger: *log.New(os.Stderr, "", 0)}
}

// Log logs the provided message to stdout if the level is higher than the current log level.
func (l *Logger) Log(level VerbosityLevel, message string) {
	if l.currentLevel < level {
		return
	}
	l.logger.Println(fmt.Sprintf("%s%s", time.Now().Format(loggingTimestampLayout), message))
}

// Logf formats and logs the provided message to stdout if the level is higher than the current log level.
func (l *Logger) Logf(level VerbosityLevel, format string, a ...interface{}) {
	l.Log(level, fmt.Sprintf(format, a...))
}
