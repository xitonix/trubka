package list

import (
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/list"
)

// Bullet represents a bullet list.
type Bullet struct {
	writer  list.Writer
	title   string
	caption string
}

// NewBullet creates a new bullet list.
func NewBullet() *Bullet {
	w := list.NewWriter()
	w.SetOutputMirror(os.Stdout)
	w.SetStyle(list.StyleMarkdown)
	w.Style().LinePrefix = " "
	return &Bullet{
		writer: w,
	}
}

// SetTitle sets the title of the list.
func (b *Bullet) SetTitle(title string) {
	b.title = title
}

// SetCaption sets the caption (/footer) of the list.
func (b *Bullet) SetCaption(caption string) {
	b.caption = caption
}

// Render prints out the list into stdout.
func (b *Bullet) Render() {
	if len(b.title) > 0 {
		underline := strings.Repeat("─", len(b.title))
		fmt.Printf("%s\n%s\n", b.title, underline)
	}
	b.writer.Render()
	if len(b.caption) > 0 {
		fmt.Printf("\n%s", b.caption)
	}
}

// AsTree switches the bullet list into a Tree.
func (b *Bullet) AsTree() {
	b.writer.SetStyle(list.StyleConnectedRounded)
}

// AddItem adds a new item to the list.
func (b *Bullet) AddItem(item interface{}) {
	b.writer.AppendItem(item)
}

// AddItemF adds a new formatted item to the list.
func (b *Bullet) AddItemF(format string, a ...interface{}) {
	b.writer.AppendItem(fmt.Sprintf(format, a...))
}

// Indent adds one level of indentation to the list.
func (b *Bullet) Indent() {
	b.writer.Indent()
}

// UnIndent removes one level of indentation from the list.
func (b *Bullet) UnIndent() {
	b.writer.UnIndent()
}
