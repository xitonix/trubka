package internal

import "github.com/gookit/color"

var (
	yellow = color.Warn.Render
	green  = color.Info.Render
	bold   = color.Bold.Render
	red    = color.Red.Render
)

type painter func(a ...interface{}) string

// Yellow returns the input in yellow if highlighting is enabled.
func Yellow(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, yellow, colorEnabled)
}

// Green returns the input in green if highlighting is enabled.
func Green(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, green, colorEnabled)
}

// Bold returns the input in bold if highlighting is enabled.
func Bold(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, bold, colorEnabled)
}

// Red returns the input in red if highlighting is enabled.
func Red(input interface{}, colorEnabled bool) interface{} {
	return colorIfEnabled(input, red, colorEnabled)
}

// RedIfTrue highlights the input in red, if highlighting is enabled and the evaluation function returns true.
func RedIfTrue(input interface{}, eval func() bool, colorEnabled bool) interface{} {
	return colorIfEnabled(input, red, colorEnabled && eval())
}

// GreenIfTrue highlights the input in green, if highlighting is enabled and the evaluation function returns true.
func GreenIfTrue(input interface{}, eval func() bool, colorEnabled bool) interface{} {
	return colorIfEnabled(input, green, colorEnabled && eval())
}

func colorIfEnabled(input interface{}, p painter, colorEnabled bool) interface{} {
	if colorEnabled {
		return p(input)
	}
	return input
}
