package format

import "fmt"

func TitleWithCount(title string, count int) string {
	return fmt.Sprintf("%s (%d)", title, count)
}
