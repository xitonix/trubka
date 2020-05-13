package format

import "fmt"

func TitleWithCount(title string, count int) string {
	return fmt.Sprintf("%s (%d)", title, count)
}

func NewLines(count int) {
	for i := 0; i < count; i++ {
		fmt.Println()
	}
}
