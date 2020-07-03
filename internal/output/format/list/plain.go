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
	items            []string
	title            string
	caption          string
	indent           int
	indentFirstLevel bool
}

// NewPlain creates new plain text list.
func NewPlain() *Plain {
	return &Plain{
		items:  make([]string, 0),
		indent: 0,
	}
}

// SetTitle sets the title of the list.
func (p *Plain) SetTitle(title string) {
	p.title = title
	p.indentFirstLevel = len(title) > 0
}

// SetCaption sets the caption (/footer) of the list.
func (p *Plain) SetCaption(caption string) {
	p.caption = caption
}

// Render prints out the list into stdout.
func (p *Plain) Render() {
	if len(p.title) > 0 {
		fmt.Println(p.title)
	}
	for i, item := range p.items {
		if i < len(p.items) {
			fmt.Println(item)
			continue
		}
		fmt.Print(item)
	}
	if len(p.caption) > 0 {
		fmt.Printf("\n%s", p.caption)
	}
}

// AsTree no-op for a plain text list.
func (p *Plain) AsTree() {
	// no ops for plain lists
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
	indent := p.indent
	if p.indentFirstLevel {
		indent++
	}
	p.items = append(p.items, strings.Repeat(indentation, indent)+fmt.Sprintf(format, a...))
}
