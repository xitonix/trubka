package output

import (
	"fmt"
)

func NewLines(count int) {
	for i := 0; i < count; i++ {
		fmt.Println()
	}
}
