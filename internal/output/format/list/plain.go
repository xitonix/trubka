package list

import (
	"fmt"
	"strings"
)

const (
	indentation = "  "
)

type Plain struct {
	items  []string
	indent int
}

func NewPlain() *Plain {
	return &Plain{
		items:  make([]string, 0),
		indent: 0,
	}
}

func (p *Plain) SetTitle(string) {
	// no ops for plain lists
}

func (p *Plain) SetCaption(string) {
	// no ops for plain lists
}

func (p *Plain) Render() {
	for i, item := range p.items {
		if i < len(p.items) {
			fmt.Println(item)
			continue
		}
		fmt.Print(item)
	}
}

func (p *Plain) AsTree() {
	// no ops for plain lists
}

func (p *Plain) AddItem(item interface{}) {
	p.indentF("%v", item)
}

func (p *Plain) AddItemF(format string, a ...interface{}) {
	p.indentF(format, a...)
}

func (p *Plain) Intend() {
	p.indent++
}

func (p *Plain) UnIntend() {
	if p.indent > 0 {
		p.indent--
	}
}

func (p *Plain) indentF(format string, a ...interface{}) {
	p.items = append(p.items, strings.Repeat(indentation, p.indent)+fmt.Sprintf(format, a...))
}
