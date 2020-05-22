package tabular

import (
	"os"
	"runtime"

	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
)

type Table struct {
	writer table.Writer
	style  *table.Style
}

func NewTable(enableColor bool, columns ...*Column) *Table {
	t := table.NewWriter()
	if runtime.GOOS == "windows" {
		t.SetStyle(table.StyleLight)
	} else {
		t.SetStyle(table.StyleRounded)
	}
	t.SetOutputMirror(os.Stdout)
	headers := make(table.Row, len(columns))
	configs := make([]table.ColumnConfig, len(columns))
	for i, column := range columns {
		headers[i] = column.Header
		configs[i] = column.configuration(enableColor)
	}
	t.AppendHeader(headers)
	t.SetColumnConfigs(configs)
	style := t.Style()
	style.Title.Align = text.AlignLeft
	style.Options.SeparateRows = true
	style.Format.Header = text.FormatDefault
	style.Format.Footer = text.FormatDefault
	return &Table{
		writer: t,
		style:  style,
	}
}

func (t *Table) TitleAlignment(alignment Alignment) {
	t.style.Title.Align = text.Align(alignment)
}

func (t *Table) AddRow(values ...interface{}) {
	row := make(table.Row, len(values))
	for i, value := range values {
		row[i] = value
	}
	t.writer.AppendRow(row)
}

func (t *Table) SetTitle(title string) {
	t.writer.SetTitle(title)
}

func (t *Table) SetCaption(caption string) {
	t.writer.SetCaption(caption)
}

func (t *Table) DisableRowSeparators() {
	t.style.Options.SeparateRows = false
}

// AddFooter use "" for the columns without any footer value.
func (t *Table) AddFooter(values ...interface{}) {
	row := make(table.Row, len(values))
	for i, value := range values {
		row[i] = value
	}
	t.writer.AppendFooter(row)
}

func (t *Table) Render() {
	t.writer.Render()
}
