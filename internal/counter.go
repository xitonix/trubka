package internal

import (
	"fmt"
	"os"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
)

type stats struct {
	success int64
	failure int64
}
type Counter struct {
	topicStats map[string]*stats
}

func NewCounter() *Counter {
	return &Counter{
		topicStats: make(map[string]*stats),
	}
}

func (c *Counter) PrintAsTable(highlight bool) {
	if c == nil || len(c.topicStats) == 0 {
		return
	}
	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"Topic", "Succeeded", "Failed"}
	table.SetHeader(headers)
	table.SetColumnAlignment([]int{
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_CENTER,
		tablewriter.ALIGN_CENTER,
	})
	table.SetRowLine(true)

	rows := make([][]string, 0)

	for topic, s := range c.topicStats {
		failed := RedIfTrue(humanize.Comma(s.failure), func() bool {
			return s.failure > 0
		}, highlight)

		succeeded := GreenIfTrue(humanize.Comma(s.success), func() bool {
			return s.success > 0
		}, highlight)
		rows = append(rows, []string{topic, fmt.Sprint(succeeded), fmt.Sprint(failed)})
	}
	fmt.Print("\nSUMMARY\n")
	table.AppendBulk(rows)
	table.Render()
}

func (c *Counter) IncrSuccess(topic string) {
	if _, ok := c.topicStats[topic]; !ok {
		c.topicStats[topic] = &stats{}
	}
	c.topicStats[topic].success++
}

func (c *Counter) IncrFailure(topic string) {
	if _, ok := c.topicStats[topic]; !ok {
		c.topicStats[topic] = &stats{}
	}
	c.topicStats[topic].failure++
}
