package list

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

type topics struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	topicFilter  *regexp.Regexp
	format       string
	style        string
}

func addTopicsSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &topics{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("topics", "Loads the existing topics from the server.").Action(cmd.run)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").
		Short('t').
		RegexpVar(&cmd.topicFilter)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
}

func (c *topics) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	topics, err := manager.GetTopics(ctx, c.topicFilter)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		return internal.NotFoundError("topic", "topic", c.topicFilter)
	}

	sort.Sort(kafka.TopicsByName(topics))

	switch c.format {
	case commands.ListFormat:
		c.printAsList(topics, false)
	case commands.TableFormat:
		c.printAsTable(topics)
	case commands.PlainTextFormat:
		c.printAsList(topics, true)
	}
	return nil
}

func (c *topics) printAsList(topics []kafka.Topic, plain bool) {
	b := list.New(plain)
	b.SetTitle(format.WithCount("Topics", len(topics)))
	var totalPartitions int64
	for _, topic := range topics {
		totalPartitions += int64(topic.NumberOfPartitions)
		b.AddItem(topic.Name)
	}
	caption := fmt.Sprintf("SUMMARY: %s partitions in %s topics", humanize.Comma(totalPartitions), humanize.Comma(int64(len(topics))))
	b.SetCaption(caption)
	b.Render()
}

func (c *topics) printAsTable(topics []kafka.Topic) {
	table := tabular.NewTable(c.globalParams.EnableColor,
		tabular.C("Topic").Align(tabular.AlignLeft),
		tabular.C("Number of Partitions").FAlign(tabular.AlignCenter),
		tabular.C("Replication Factor"),
	)
	table.SetTitle(format.WithCount("Topics", len(topics)))

	var totalPartitions int64
	for _, topic := range topics {
		totalPartitions += int64(topic.NumberOfPartitions)
		table.AddRow(topic.Name, topic.NumberOfPartitions, topic.ReplicationFactor)
	}
	table.AddFooter(fmt.Sprintf("Total: %s", humanize.Comma(int64(len(topics)))), humanize.Comma(totalPartitions), " ")
	table.Render()
}
