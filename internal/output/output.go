package output

import (
	"encoding/json"
	"fmt"

	"github.com/xitonix/trubka/internal"
)

func NewLines(count int) {
	for i := 0; i < count; i++ {
		fmt.Println()
	}
}

func PrintAsJson(data interface{}, style string, enableColor bool) error {
	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	h := internal.NewJsonHighlighter(style, enableColor)
	fmt.Println(string(h.Highlight(result)))
	return nil
}
