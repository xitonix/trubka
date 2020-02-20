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

func UnderlineWithCount(title string, count int) {
	title = Underline(titledCounter(title, count))
	fmt.Printf("\n%s\n", title)
}

func WithCount(title string, count int) {
	fmt.Printf("\n%s\n", titledCounter(title, count))
}

func titledCounter(title string, count int) string {
	return fmt.Sprintf("%s (%d)", title, count)
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
