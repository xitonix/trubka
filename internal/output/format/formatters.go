package format

import (
	"fmt"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/jedib0t/go-pretty/text"
)

const stableGroupLabel = "Stable"

var (
	yellow = text.FgHiYellow
	green  = text.FgHiGreen
	bold   = text.Bold
	red    = text.FgHiRed
)

// GreenLabel returns a decorated green label if colours are enabled, otherwise returns "[val]".
func GreenLabel(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return fmt.Sprintf("[%v]", val)
	}
	return text.Colors{text.Bold, text.BgGreen, text.FgWhite}.Sprintf(" %v ", val)
}

// Warn returns a yellow warning message if colours are enabled.
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

// GroupStateLabel returns a decorated consumer group state label if colours are enabled.
func GroupStateLabel(state string, enableColor bool) string {
	if strings.EqualFold(state, stableGroupLabel) {
		return fmt.Sprint(GreenLabel(stableGroupLabel, enableColor))
	}
	return state
}

// BoldGreen returns a bold green string if colours are enabled.
func BoldGreen(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return val
	}
	return text.Colors{text.Bold, text.FgHiGreen}.Sprint(val)
}

// SpaceIfEmpty returns a single whitespace if the input is an empty string, otherwise returns the input.
func SpaceIfEmpty(in string) string {
	if len(in) > 0 {
		return in
	}
	return " "
}

// Yellow returns the input in yellow if coloring is enabled.
func Yellow(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, yellow, colorEnabled)
}

// Red returns the input in red if coloring is enabled.
func Red(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, red, colorEnabled)
}

// RedIfTrue highlights the input in red, if coloring is enabled and the evaluation function returns true.
func RedIfTrue(input interface{}, eval func() bool, colorEnabled bool) interface{} {
	return colorIfEnabled(input, red, colorEnabled && eval())
}

// GreenIfTrue highlights the input in green, if coloring is enabled and the evaluation function returns true.
func GreenIfTrue(input interface{}, eval func() bool, colorEnabled bool) interface{} {
	return colorIfEnabled(input, green, colorEnabled && eval())
}

// Underline returns the underlined text.
func Underline(input string) string {
	return underlineLen(input, len(input))
}

// UnderlinedTitleWithCount returns the underlined input in "title [count]" format.
func UnderlinedTitleWithCount(title string, count int) string {
	title = titleWithCount(title, count)
	return fmt.Sprintf("%s\n%s", title, underline(len(title)))
}

// WithCount returns the input in "title [count]" format.
func WithCount(title string, count int) string {
	return titleWithCount(title, count)
}

func titleWithCount(title string, count int) string {
	return fmt.Sprintf("%s (%d)", title, count)
}

func underline(length int) string {
	return strings.Repeat("─", length)
}

func underlineLen(input string, length int) string {
	return fmt.Sprintf("%s\n%s", input, underline(length))
}

func colorIfEnabled(input interface{}, color text.Color, colorEnabled bool) interface{} {
	if colorEnabled {
		return text.Colors{color}.Sprint(input)
	}
	return input
}
