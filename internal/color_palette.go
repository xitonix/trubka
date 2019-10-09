package internal

import "github.com/gookit/color"

var (
	Yellow = color.Warn.Render
	Green  = color.Info.Render
	Bold   = color.Bold.Render
	Red    = color.Red.Render
)

// RedIfTrue highlights the input in red, if the evaluation function returns true.
func RedIfTrue(input interface{}, eval func() bool) string {
	if eval() {
		return Red(input)
	}
	return color.Normal.Render(input)
}

// GreenIfTrue highlights the input in green, if the evaluation function returns true.
func GreenIfTrue(input interface{}, eval func() bool) string {
	if eval() {
		return Green(input)
	}
	return color.Normal.Render(input)
}
