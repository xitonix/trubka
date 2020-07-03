package internal

import (
	"github.com/dustin/go-humanize"

	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/tabular"
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
	table := tabular.NewTable(highlight,
		tabular.C("Topic").Align(tabular.AlignLeft),
		tabular.C("Succeeded"),
		tabular.C("Failed"))

	for topic, s := range c.topicStats {
		failed := format.RedIfTrue(humanize.Comma(s.failure), func() bool {
			return s.failure > 0
		}, highlight)

		succeeded := format.GreenIfTrue(humanize.Comma(s.success), func() bool {
			return s.success > 0
		}, highlight)
		table.AddRow(topic, succeeded, failed)
	}
	table.SetTitle("SUMMARY")
	table.TitleAlignment(tabular.AlignCenter)
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
