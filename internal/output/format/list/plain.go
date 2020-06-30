package list

import (
	"fmt"
	"strings"
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
	value := fmt.Sprintf(format, a...)
	if p.indent == 0 && len(p.title) > 0 {
		value = "  " + value
	}
	p.items = append(p.items, strings.Repeat("  ", p.indent)+value)
}
