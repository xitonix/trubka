package commands

import (
	"fmt"
	"regexp"

	"github.com/gookit/color"
)

const (
	plainTextFormat = "plain"
	tableFormat     = "table"
)

var (
	yellow = color.Warn.Render
	green  = color.Info.Render
	bold   = color.Bold.Render
)

func highlightLag(input int64) string {
	if input > 0 {
		return yellow(input)
	}
	return green(input)
}

func getNotFoundMessage(entity, filterName string, ex *regexp.Regexp) string {
	msg := fmt.Sprintf("No %s has been found.", entity)
	if ex != nil {
		msg += fmt.Sprintf(" You might need to tweak the %s filter (%s).", filterName, ex.String())
	}
	return msg
}
