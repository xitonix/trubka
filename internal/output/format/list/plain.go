package list

import (
	"fmt"
	"strings"
)

const (
	indentation = "  "
)

type Plain struct {
	items   []string
	title   string
	caption string
	indent  int
}

func NewPlain() *Plain {
	return &Plain{
		items:  make([]string, 0),
		indent: 0,
	}
}

func (p *Plain) SetTitle(title string) {
	p.title = title
	if p.indent == 0 && len(title) > 0 {
		// Make sure we always indent if the title is set to achieve:
		// Title
		//   item 1
		//   item 2
		p.indent = 1
	}
}

func (p *Plain) SetCaption(caption string) {
	p.caption = caption
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
	if len(p.caption) > 0 {
		fmt.Printf("\n%s", p.caption)
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
