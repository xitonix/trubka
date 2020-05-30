package format

import (
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/text"
)

func Bold(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return val
	}
	return text.Bold.Sprint(val)
}

func GreenLabel(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return fmt.Sprintf("[%v]", val)
	}
	return text.Colors{text.Bold, text.BgGreen, text.FgWhite}.Sprintf(" %v ", val)
}

func Warn(input int64, colorEnabled, greenOtherwise bool) interface{} {
	humanised := humanize.Comma(input)
	if !colorEnabled {
		return humanised
	}
	if input > 0 {
		return text.Colors{text.FgHiYellow, text.Bold}.Sprint(humanised)
	}
	if greenOtherwise {
		return text.Colors{text.FgHiGreen, text.Bold}.Sprint(humanised)
	}
	return humanised
}

func BoldGreen(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return val
	}
	return text.Colors{text.Bold, text.FgHiGreen}.Sprint(val)
}

func UnderlineLen(input string, length int) string {
	return fmt.Sprintf("%s\n%s", input, underline(length))
}

func Underline(input string) string {
	return UnderlineLen(input, len(input))
}

func UnderlinedTitleWithCount(title string, count int) string {
	title = titleWithCount(title, count)
	return fmt.Sprintf("%s\n%s", title, underline(len(title)))
}

func WithCount(title string, count int) string {
	return titleWithCount(title, count)
}

func titleWithCount(title string, count int) string {
	return fmt.Sprintf("%s (%d)", title, count)
}

func underline(length int) string {
	return strings.Repeat("─", length)
}

func SpaceIfEmpty(in string) string {
	if len(in) > 0 {
		return in
	}
	return " "
}
