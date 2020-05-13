package tabular

import (
	"fmt"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/table"
	"github.com/jedib0t/go-pretty/text"
)

type Alignment int

const (
	AlignLeft Alignment = iota + 1
	AlignCenter
	AlignJustify
	AlignRight
)

type VAlignment int

const (
	VAlignTop Alignment = iota + 1
	VAlignMiddle
	VAlignBottom
)

type Column struct {
	Header         string
	humanize       bool
	greenOtherwise bool
	warningLevel   *int64
	config         table.ColumnConfig
}

func C(header string) *Column {
	return &Column{
		Header: header,
		config: table.ColumnConfig{
			Name:        header,
			Align:       text.Align(AlignCenter),
			AlignHeader: text.Align(AlignCenter),
			VAlign:      text.VAlign(VAlignTop),
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

func (c *Column) Humanize() *Column {
	c.humanize = true
	return c
}

func (c *Column) Warn(level int64, greenOtherwise bool) *Column {
	c.warningLevel = &level
	c.greenOtherwise = greenOtherwise
	return c
}

func (c *Column) HAlign(alignment Alignment) *Column {
	c.config.AlignHeader = text.Align(alignment)
	return c
}

func (c *Column) FAlign(alignment Alignment) *Column {
	c.config.AlignFooter = text.Align(alignment)
	return c
}

func (c *Column) Align(alignment Alignment) *Column {
	c.config.Align = text.Align(alignment)
	return c
}

func (c *Column) VAlign(alignment VAlignment) *Column {
	c.config.VAlign = text.VAlign(alignment)
	return c
}

func (c *Column) MinWidth(width int) *Column {
	c.config.WidthMin = width
	return c
}

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
