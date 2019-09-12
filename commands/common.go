package commands

import "github.com/gookit/color"

const (
	plainTextFormat = "plain"
	tableFormat     = "table"
)

var (
	warn = color.Warn.Render
	info = color.Info.Render
)

func highlightLag(input int64) string {
	if input > 0 {
		return warn(input)
	}
	return info(input)
}
