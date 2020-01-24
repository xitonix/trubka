package output

import (
	"fmt"
	"io"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func Underline(in string) string {
	in = strings.TrimSpace(in)
	if len(in) == 0 {
		return ""
	}
	return in + "\n" + strings.Repeat("-", len(in))
}

func UnderlineWithCount(title string, count int) string {
	title = fmt.Sprintf("%s (%d)", title, count)
	return Underline(title)
}

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

func SpaceIfEmpty(in string) string {
	if len(in) > 0 {
		return in
	}
	return " "
}
