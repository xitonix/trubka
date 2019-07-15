package internal

import (
	"fmt"
	"sync"
)

// Printer represents a printer type
type Printer interface {
	Write(level VerbosityLevel, msg string)
	Writef(level VerbosityLevel, format string, args ...interface{})
	Writeln(level VerbosityLevel, msg string)
	Level() VerbosityLevel
}

// SyncPrinter is an implementation of Printer interface to synchronously write to Stdout buffer.
type SyncPrinter struct {
	mux          sync.Mutex
	currentLevel VerbosityLevel
}

// NewPrinter creates a new synchronised Stdout writer.
func NewPrinter(currentLevel VerbosityLevel) *SyncPrinter {
	return &SyncPrinter{
		currentLevel: currentLevel,
	}
}

// Write writes to standard output synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Write(level VerbosityLevel, msg string) {
	if p.currentLevel < level {
		return
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	fmt.Print(msg)
}

// Writeln writes a new line to standard output synchronously if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Writeln(level VerbosityLevel, msg string) {
	if p.currentLevel < level {
		return
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	fmt.Println(msg)
}

// Writef formats according to a format specifier and writes to standard output synchronously,
// if the verbosity level is greater than or equal to the current level.
func (p *SyncPrinter) Writef(level VerbosityLevel, format string, a ...interface{}) {
	if p.currentLevel < level {
		return
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	fmt.Printf(format, a...)
}

// Level returns the current verbosity level.
func (p *SyncPrinter) Level() VerbosityLevel {
	return p.currentLevel
}
