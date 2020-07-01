package list

import (
	"fmt"
	"strings"
)

const (
	indentation = "  "
)

type Plain struct {
	items            []string
	title            string
	indent           int
	indentFirstLevel bool
}

func NewPlain() *Plain {
	return &Plain{
		items:  make([]string, 0),
		indent: 0,
	}
}

func (p *Plain) SetTitle(title string) {
	p.title = title
	p.indentFirstLevel = len(title) > 0
}

func (p *Plain) SetCaption(caption string) {
	// no ops for plain lists
}

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
	indent := p.indent
	if p.indentFirstLevel {
		indent++
	}
	p.items = append(p.items, strings.Repeat(indentation, indent)+fmt.Sprintf(format, a...))
}
