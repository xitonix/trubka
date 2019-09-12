package internal

import (
	"fmt"
	"io"
	"sync"
	"time"
)

const (
	loggingWriterKey       = "___trubka__logging__writer__key___"
	loggingTimestampLayout = "2006/01/02 15:04:05 "
)

// Printer represents a printer type.
type Printer interface {
	Logf(level VerbosityLevel, format string, args ...interface{})
	Log(level VerbosityLevel, msg string)
	Errorf(level VerbosityLevel, format string, args ...interface{})
	Error(level VerbosityLevel, msg string)
	Infof(level VerbosityLevel, format string, args ...interface{})
	Info(level VerbosityLevel, msg string)
	Warningf(level VerbosityLevel, format string, args ...interface{})
	Warning(level VerbosityLevel, msg string)
	WriteEvent(topic string, bytes []byte)
	Close()
	Level() VerbosityLevel
}

// SyncPrinter is an implementation of Printer interface to synchronously write to specified io.Writer instances.
type SyncPrinter struct {
	currentLevel  VerbosityLevel
	wg            sync.WaitGroup
	targets       map[string]chan string
	uniqueTargets map[io.Writer]chan string
}

// NewPrinter creates a new synchronised writer.
func NewPrinter(currentLevel VerbosityLevel, logOutput io.Writer) *SyncPrinter {
	logInput := make(chan string, 100)
	return &SyncPrinter{
		currentLevel: currentLevel,
		uniqueTargets: map[io.Writer]chan string{
			logOutput: logInput,
		},
		targets: map[string]chan string{
			loggingWriterKey: logInput,
		},
	}
}

// Start starts the underlying message processors.
func (p *SyncPrinter) Start(messageOutputs map[string]io.Writer) {
	for topic, writer := range messageOutputs {
		input, ok := p.uniqueTargets[writer]
		if !ok {
			input = make(chan string)
			p.uniqueTargets[writer] = input
		}

		p.targets[topic] = input
	}

	for w, in := range p.uniqueTargets {
		p.wg.Add(1)
		go func(writer io.Writer, input chan string) {
			defer p.wg.Done()
			for msg := range input {
				_, err := fmt.Fprintln(writer, msg)
				if err != nil {
					fmt.Printf("Failed to write the entry: %s\n", err)
				}
			}
		}(w, in)
	}
}

// Close closes the internal synchronisation channels.
//
// Writing into a closed printer will panic.
func (p *SyncPrinter) Close() {
	for _, inputChannel := range p.uniqueTargets {
		close(inputChannel)
	}
	p.wg.Wait()
}

// Log writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Log(level VerbosityLevel, msg string) {
	p.log(level, msg)
}

// Logf formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Logf(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...))
}

// Info writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Info(level VerbosityLevel, msg string) {
	p.log(level, msg)
}

// Infof formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Infof(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...))
}

// Warning writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Warning(level VerbosityLevel, msg string) {
	p.log(level, msg)
}

// Warningf formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Warningf(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...))
}

// Error writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Error(level VerbosityLevel, msg string) {
	p.log(level, msg)
}

// Errorf formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Errorf(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...))
}

// WriteEvent writes the event content to the relevant message io.Writer.
func (p *SyncPrinter) WriteEvent(topic string, bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	p.targets[topic] <- string(bytes)
}

// Level returns the current verbosity level
func (p *SyncPrinter) Level() VerbosityLevel {
	return p.currentLevel
}

func (p *SyncPrinter) log(level VerbosityLevel, msg string) {
	if p.currentLevel < level {
		return
	}
	p.targets[loggingWriterKey] <- time.Now().Format(loggingTimestampLayout) + msg
}
