package list

import (
	"fmt"
	"os"
	"strings"

	"github.com/jedib0t/go-pretty/list"
)

type Bullet struct {
	writer  list.Writer
	title   string
	caption string
}

func NewBullet() *Bullet {
	w := list.NewWriter()
	w.SetOutputMirror(os.Stdout)
	w.SetStyle(list.StyleBulletCircle)
	w.Style().LinePrefix = " "
	return &Bullet{
		writer: w,
	}
}

func (b *Bullet) SetTitle(title string) {
	b.title = title
}

func (b *Bullet) SetCaption(caption string) {
	b.caption = caption
}

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

func (b *Bullet) AsTree() {
	b.writer.SetStyle(list.StyleConnectedRounded)
}

func (b *Bullet) AddItem(item interface{}) {
	b.writer.AppendItem(item)
}

func (b *Bullet) AddItemF(format string, a ...interface{}) {
	b.writer.AppendItem(fmt.Sprintf(format, a...))
}

func (b *Bullet) Intend() {
	b.writer.Indent()
}

func (b *Bullet) UnIntend() {
	b.writer.UnIndent()
}
