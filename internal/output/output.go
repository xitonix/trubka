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

// PrintAsJSON prints the input data into stdout as Json.
func PrintAsJSON(data interface{}, style string, enableColor bool, compact bool) error {
	result, err := marshalJson(data, compact)
	if err != nil {
		return err
	}
	h := internal.NewJSONHighlighter(style, enableColor)
	fmt.Println(string(h.Highlight(result)))
	return nil
}

func marshalJson(data interface{}, compact bool) ([]byte, error) {
	if compact {
		return json.Marshal(data)
	} else {
		return json.MarshalIndent(data, "", "  ")
	}
}
