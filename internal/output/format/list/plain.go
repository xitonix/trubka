package list

import (
	"fmt"
	"strings"
)

const (
	indentation = "  "
)

// Plain represents a plain text list.
type Plain struct {
	items  []string
	indent int
}

func newPlain() *Plain {
	return &Plain{
		items:  make([]string, 0),
		indent: 0,
	}
}

// Render prints out the list into stdout.
func (p *Plain) Render() {
	for i, item := range p.items {
		if i < len(p.items) {
			fmt.Println(item)
			continue
		}
		fmt.Print(item)
	}
}

// AddItem adds a new item to the list.
func (p *Plain) AddItem(item interface{}) {
	p.indentF("%v", item)
}

// AddItemF adds a new formatted item to the list.
func (p *Plain) AddItemF(format string, a ...interface{}) {
	p.indentF(format, a...)
}

// Indent adds one level of indentation to the list.
func (p *Plain) Indent() {
	p.indent++
}

// UnIndent removes one level of indentation from the list.
func (p *Plain) UnIndent() {
	if p.indent > 0 {
		p.indent--
	}
}

func (p *Plain) indentF(format string, a ...interface{}) {
	p.items = append(p.items, strings.Repeat(indentation, p.indent)+fmt.Sprintf(format, a...))
}
