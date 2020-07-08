package list

import (
	"fmt"
	"os"

	"github.com/jedib0t/go-pretty/list"
)

// Tree represents a tree.
type Tree struct {
	writer list.Writer
}

func newTree() *Tree {
	w := list.NewWriter()
	w.SetOutputMirror(os.Stdout)
	w.SetStyle(list.StyleConnectedRounded)
	w.Style().LinePrefix = " "
	return &Tree{
		writer: w,
	}
}

// Render prints out the list into stdout.
func (t *Tree) Render() {
	t.writer.Render()
}

// AddItem adds a new item to the list.
func (t *Tree) AddItem(item interface{}) {
	t.writer.AppendItem(item)
}

// AddItemF adds a new formatted item to the list.
func (t *Tree) AddItemF(format string, a ...interface{}) {
	t.writer.AppendItem(fmt.Sprintf(format, a...))
}

// Indent adds one level of indentation to the list.
func (t *Tree) Indent() {
	t.writer.Indent()
}

// UnIndent removes one level of indentation from the list.
func (t *Tree) UnIndent() {
	t.writer.UnIndent()
}
