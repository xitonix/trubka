package internal

import (
	"fmt"
	"strings"

	"github.com/jedib0t/go-pretty/text"
)

var (
	yellow = text.FgHiYellow
	green  = text.FgHiGreen
	bold   = text.Bold
	red    = text.FgHiRed
)

type painter func(a ...interface{}) string

// Yellow returns the input in yellow if coloring is enabled.
func Yellow(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, yellow, colorEnabled)
}

// Green returns the input in green if coloring is enabled.
func Green(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, green, colorEnabled)
}

// Bold returns the input in bold if coloring is enabled.
func Bold(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, bold, colorEnabled)
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

func HighlightGroupState(state string, colorEnabled bool) string {
	s := GreenIfTrue(state, func() bool {
		return strings.EqualFold(state, "Stable")
	}, colorEnabled)

	return fmt.Sprint(s)
}

func colorIfEnabled(input interface{}, color text.Color, colorEnabled bool) interface{} {
	if colorEnabled {
		return text.Colors{color}.Sprint(input)
	}
	return input
}
