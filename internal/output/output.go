package output

import (
	"encoding/json"
	"fmt"

	"github.com/xitonix/trubka/internal"
)

// NewLines prints `count` number of new lines to stdout.
func NewLines(count int) {
	for i := 0; i < count; i++ {
		fmt.Println()
	}
}

// PrintAsJson prints the input data into stdout as Json.
func PrintAsJson(data interface{}, style string, enableColor bool) error {
	result, err := json.MarshalIndent(data, "", "  ")
	if err != nil {
		return err
	}
	h := internal.NewJsonHighlighter(style, enableColor)
	fmt.Println(string(h.Highlight(result)))
	return nil
}
