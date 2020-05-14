package output

import (
	"fmt"
	"io"

	"github.com/olekukonko/tablewriter"
)

func InitStaticTable(writer io.Writer, headers ...TableHeader) *tablewriter.Table {
	table := tablewriter.NewWriter(writer)
	headerTitles := make([]string, len(headers))
	alignments := make([]int, len(headers))
	var i int
	for _, header := range headers {
		headerTitles[i] = header.Key
		alignments[i] = header.Alignment
		i++
	}
	table.SetHeader(headerTitles)
	table.SetColumnAlignment(alignments)
	table.SetAutoWrapText(false)
	table.SetRowLine(true)
	return table
}

func NewLines(count int) {
	for i := 0; i < count; i++ {
		fmt.Println()
	}
}
