package format

import "github.com/jedib0t/go-pretty/text"

func Bold(val interface{}, enableColor bool) interface{} {
	if !enableColor {
		return val
	}
	return text.Bold.Sprint(val)
}

func SpaceIfEmpty(in string) string {
	if len(in) > 0 {
		return in
	}
	return " "
}
