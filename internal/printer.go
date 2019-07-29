package internal

import (
	"fmt"
	"io"
	"sync"
)

// Printer represents a printer type
type Printer interface {
	Logf(level VerbosityLevel, format string, args ...interface{})
	Log(level VerbosityLevel, msg string)
	WriteMessage(bytes []byte)
	Level() VerbosityLevel
	Close()
}

type entry struct {
	writer io.Writer
	value  interface{}
}

// SyncPrinter is an implementation of Printer interface to synchronously write to specified io.Writer instances.
type SyncPrinter struct {
	currentLevel  VerbosityLevel
	wg            sync.WaitGroup
	mux           sync.Mutex
	isClosed      bool
	input         chan *entry
	logOutput     io.Writer
	messageOutput io.Writer
}

// NewPrinter creates a new synchronised writer.
func NewPrinter(currentLevel VerbosityLevel, logOutput, messageOutput io.Writer) *SyncPrinter {
	p := &SyncPrinter{
		currentLevel:  currentLevel,
		logOutput:     logOutput,
		messageOutput: messageOutput,
		input:         make(chan *entry, 100),
	}

	p.wg.Add(1)
	go p.writeEntities()
	return p
}

// Close closes the internal synchronisation channels.
//
// Writing into a closed printer will have no effect.
func (p *SyncPrinter) Close() {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.isClosed {
		return
	}
	p.isClosed = true
	close(p.input)
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

// Level returns the current verbosity level.
func (p *SyncPrinter) Level() VerbosityLevel {
	return p.currentLevel
}

// WriteMessage writes the message to the Message io.Writer.
func (p *SyncPrinter) WriteMessage(bytes []byte) {
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.isClosed {
		return
	}
	p.input <- &entry{
		writer: p.messageOutput,
		value:  string(bytes) + "\n",
	}
}

func (p *SyncPrinter) log(level VerbosityLevel, msg string) {
	if p.currentLevel < level {
		return
	}
	p.mux.Lock()
	defer p.mux.Unlock()
	if p.isClosed {
		return
	}
	p.input <- &entry{
		writer: p.logOutput,
		value:  msg + "\n",
	}
}

func (p *SyncPrinter) writeEntities() {
	defer p.wg.Done()
	for entry := range p.input {
		_, err := fmt.Fprint(entry.writer, entry.value)
		if err != nil {
			fmt.Printf("Failed to write the entry: %s\n", err)
		}
	}
}
