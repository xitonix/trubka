package format

import (
	"fmt"
	"strings"

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

func BoldGreen(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return val
	}
	return text.Colors{text.Bold, text.FgHiGreen}.Sprint(val)
}

func Underline(input string) string {
	return fmt.Sprintf("%s\n%s", input, underline(len(input)))
}

func UnderlinedTitleWithCount(title string, count int) string {
	title = titleWithCount(title, count)
	return fmt.Sprintf("%s\n%s", title, underline(len(title)))
}

func WithCount(title string, count int) string {
	return fmt.Sprintf("%s", titleWithCount(title, count))
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
