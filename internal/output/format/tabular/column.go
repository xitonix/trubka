package tabular

import (
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
)

// Alignment content horizontal alignment.
type Alignment int

const (
	// AlignLeft align the content to the left.
	AlignLeft Alignment = iota + 1
	// AlignCenter align the content to the centre.
	AlignCenter
	// AlignRight align the content to the right.
	AlignRight
)

// Column represents a table column.
type Column struct {
	Header         string
	humanize       bool
	greenOtherwise bool
	warningLevel   *int64
	config         table.ColumnConfig
}

// C creates a new table column.
func C(header string) *Column {
	return &Column{
		Header: header,
		config: table.ColumnConfig{
			Name:        header,
			Align:       text.Align(AlignCenter),
			AlignHeader: text.Align(AlignCenter),
			VAlign:      text.VAlignMiddle,
			AlignFooter: text.Align(AlignRight),
		},
	}
}

func (c *Column) configuration(enableColor bool) table.ColumnConfig {
	if c.humanize || c.warningLevel != nil {
		transformer := func(val interface{}) string {
			switch value := val.(type) {
			case int:
				return c.renderNumber(enableColor, int64(value))
			case int64:
				return c.renderNumber(enableColor, value)
			}

			return fmt.Sprint(val)
		}
		c.config.Transformer = transformer
		c.config.TransformerFooter = transformer
	}
	return c.config
}

// Humanize enables comma separation of the digits for numeric columns.
func (c *Column) Humanize() *Column {
	c.humanize = true
	return c
}

// Warn sets the warning level for the numeric columns.
func (c *Column) Warn(level int64, greenOtherwise bool) *Column {
	c.warningLevel = &level
	c.greenOtherwise = greenOtherwise
	return c
}

// HAlign sets the horizontal alignment of the header.
func (c *Column) HAlign(alignment Alignment) *Column {
	c.config.AlignHeader = text.Align(alignment)
	return c
}

// FAlign sets the horizontal alignment of the footer.
func (c *Column) FAlign(alignment Alignment) *Column {
	c.config.AlignFooter = text.Align(alignment)
	return c
}

// Align sets the horizontal alignment of the cell content.
func (c *Column) Align(alignment Alignment) *Column {
	c.config.Align = text.Align(alignment)
	return c
}

// MinWidth sets the column's minimum width.
func (c *Column) MinWidth(width int) *Column {
	c.config.WidthMin = width
	return c
}

// MaxWidth sets the column's maximum width.
func (c *Column) MaxWidth(width int) *Column {
	c.config.WidthMax = width
	return c
}

func (c *Column) renderNumber(enableColor bool, value int64) string {
	var rendered string
	if c.humanize {
		rendered = humanize.Comma(value)
	} else {
		rendered = strconv.FormatInt(value, 10)
	}
	if enableColor && c.warningLevel != nil {
		if value > *c.warningLevel {
			return text.Colors{text.FgHiYellow, text.Bold}.Sprint(rendered)
		}
		if c.greenOtherwise {
			return text.Colors{text.FgHiGreen, text.Bold}.Sprint(rendered)
		}
	}

	return rendered
}
