package internal

import (
	"fmt"
	"time"
)

type Logger struct {
	currentLevel VerbosityLevel
}

func NewLogger(level VerbosityLevel) *Logger {
	return &Logger{currentLevel: level}
}

func (l *Logger) Log(level VerbosityLevel, message string) {
	if l.currentLevel < level {
		return
	}
	fmt.Println(time.Now().Format(loggingTimestampLayout) + message)
}

func (l *Logger) Logf(level VerbosityLevel, format string, a ...interface{}) {
	l.Log(level, fmt.Sprintf(format, a...))
}
