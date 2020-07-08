package list

import (
	"fmt"
	"regexp"
	"strconv"

	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

type groupOffset struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	group        string
	topicFilter  *regexp.Regexp
	format       string
	style        string
}

func addGroupOffsetsSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &groupOffset{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("group-offsets", "Lists a consumer group's offsets for all the topics within the group.").Action(cmd.run)
	c.Arg("group", "The consumer group name to fetch the offsets for.").Required().StringVar(&cmd.group)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").
		Short('t').
		RegexpVar(&cmd.topicFilter)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
}

func (g *groupOffset) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(g.globalParams, g.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	topics, err := manager.GetGroupOffsets(ctx, g.group, g.topicFilter)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		return internal.NotFoundError("topic", "topic", g.topicFilter)
	}

	switch g.format {
	case commands.JsonFormat:
		return output.PrintAsJson(topics.ToJson(), g.style, g.globalParams.EnableColor)
	case commands.TableFormat:
		return g.printAsTable(topics)
	case commands.TreeFormat:
		return g.printAsList(topics, false)
	case commands.PlainTextFormat:
		return g.printAsList(topics, true)
	default:
		return nil
	}
}

func (g *groupOffset) printAsTable(topics kafka.TopicPartitionOffset) error {
	for topic, partitionOffsets := range topics {
		table := tabular.NewTable(g.globalParams.EnableColor,
			tabular.C("Partition").MinWidth(10),
			tabular.C("Latest").MinWidth(10).Align(tabular.AlignCenter),
			tabular.C("Current").MinWidth(10).Align(tabular.AlignCenter),
			tabular.C("Lag").MinWidth(10).Humanize().FAlign(tabular.AlignCenter).Warn(0, true),
		)

		table.SetTitle(fmt.Sprintf("Topic: %s", topic))
		if len(partitionOffsets) > 0 {
			partitions := partitionOffsets.SortPartitions()
			var totalLag int64
			for _, partition := range partitions {
				offsets := partitionOffsets[int32(partition)]
				lag := offsets.Lag()
				totalLag += lag
				latest := humanize.Comma(offsets.Latest)
				current := humanize.Comma(offsets.Current)
				part := strconv.FormatInt(int64(partition), 10)
				table.AddRow(part, latest, current, lag)
			}
			table.AddFooter(" ", " ", " ", totalLag)
			table.Render()
		}
	}
	return nil
}

func (g *groupOffset) printAsList(topics kafka.TopicPartitionOffset, plain bool) error {
	l := list.New(plain)
	if !plain {
		l.AddItem(g.group)
		l.Indent()
	}
	for topic, partitionOffsets := range topics {
		l.AddItem(topic)
		var totalLag int64
		if len(partitionOffsets) > 0 {
			partitions := partitionOffsets.SortPartitions()
			l.Indent()
			for _, partition := range partitions {
				offsets := partitionOffsets[int32(partition)]
				lag := offsets.Lag()
				totalLag += lag
				l.AddItemF("P%d", partition)
				l.Indent()
				l.AddItemF(" Latest: %s", humanize.Comma(offsets.Latest))
				l.AddItemF("Current: %s", humanize.Comma(offsets.Current))
				l.AddItemF("    Lag: %v", format.Warn(lag, g.globalParams.EnableColor && !plain, true))
				l.UnIndent()
			}
			l.UnIndent()
		}
	}
	l.UnIndent()
	l.Render()
	return nil
}
