package internal

import (
	"fmt"
	"io"
	"sync"

	"github.com/gookit/color"
)

const (
	loggingWriterKey = "___trubka__logging__writer__key___"
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
}

type ColorTheme struct {
	Error   color.Style
	Info    color.Style
	Warning color.Style
}

type printable struct {
	msg   string
	style color.Style
}

// SyncPrinter is an implementation of Printer interface to synchronously write to specified io.Writer instances.
type SyncPrinter struct {
	currentLevel  VerbosityLevel
	wg            sync.WaitGroup
	targets       map[string]chan *printable
	uniqueTargets map[io.Writer]chan *printable
	theme         ColorTheme
}

// NewPrinter creates a new synchronised writer.
func NewPrinter(currentLevel VerbosityLevel, logOutput io.Writer, theme ColorTheme) *SyncPrinter {
	logInput := make(chan *printable, 100)
	return &SyncPrinter{
		theme:        theme,
		currentLevel: currentLevel,
		uniqueTargets: map[io.Writer]chan *printable{
			logOutput: logInput,
		},
		targets: map[string]chan *printable{
			loggingWriterKey: logInput,
		},
	}
}

// Start starts the underlying message processors.
func (p *SyncPrinter) Start(messageOutputs map[string]io.Writer) {
	for topic, writer := range messageOutputs {
		input, ok := p.uniqueTargets[writer]
		if !ok {
			input = make(chan *printable)
			p.uniqueTargets[writer] = input
		}

		p.targets[topic] = input
	}

	for w, in := range p.uniqueTargets {
		p.wg.Add(1)
		go func(writer io.Writer, input chan *printable) {
			defer p.wg.Done()
			for p := range input {
				var err error
				if p.style == nil {
					_, err = fmt.Fprintln(writer, p.msg)
				} else {
					p.style.Println(p.msg)
				}
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
	p.log(level, msg, nil)
}

// Logf formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Logf(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...), nil)
}

// Info writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Info(level VerbosityLevel, msg string) {
	p.log(level, msg, p.theme.Info)
}

// Infof formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Infof(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...), p.theme.Info)
}

// Warning writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Warning(level VerbosityLevel, msg string) {
	p.log(level, msg, p.theme.Warning)
}

// Warningf formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Warningf(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...), p.theme.Warning)
}

// Error writes a new line to the Logging io.Writer synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Error(level VerbosityLevel, msg string, ) {
	p.log(level, msg, p.theme.Error)
}

// Errorf formats according to a format specifier and writes a new line to the Logging io.Writer synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Errorf(level VerbosityLevel, format string, a ...interface{}) {
	p.log(level, fmt.Sprintf(format, a...), p.theme.Error)
}

// WriteEvent writes the event content to the relevant message io.Writer.
func (p *SyncPrinter) WriteEvent(topic string, bytes []byte) {
	if len(bytes) == 0 {
		return
	}
	p.targets[topic] <- &printable{
		msg: string(bytes),
	}
}

func (p *SyncPrinter) log(level VerbosityLevel, msg string, style color.Style) {
	if p.currentLevel < level {
		return
	}
	p.targets[loggingWriterKey] <- &printable{
		msg:   msg,
		style: style,
	}
}
