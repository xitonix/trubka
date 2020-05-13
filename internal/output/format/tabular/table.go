package tabular

import (
	"io"

	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
)

type Table struct {
	table table.Writer
	style *table.Style
}

func NewTable(output io.Writer, enableColor bool, columns ...*Column) *Table {
	t := table.NewWriter()
	t.SetStyle(table.StyleRounded)
	t.SetOutputMirror(output)
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
		table: t,
		style: style,
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
	t.table.AppendRow(row)
}

func (t *Table) SetTitle(format string, a ...interface{}) {
	t.table.SetTitle(format, a...)
}

func (t *Table) SetCaption(format string, a ...interface{}) {
	t.table.SetCaption(format, a...)
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
	t.table.AppendFooter(row)
}

func (t *Table) Render() {
	t.table.Render()
}
