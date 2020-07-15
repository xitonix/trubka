package list

import (
	"fmt"
	"os"

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

type listLocalOffsets struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters
	topic        string
	environment  string
	format       string
	style        string
}

func addLocalOffsetsSubCommand(parent *kingpin.CmdClause, params *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &listLocalOffsets{
		globalParams: params,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("local-offsets", "Lists the locally stored offsets of the given topic and environment.").Action(cmd.run)
	c.Arg("topic", "The topic to loads the local offsets of.").Required().StringVar(&cmd.topic)
	c.Arg("environment", "The environment to load the topic offset from.").Required().StringVar(&cmd.environment)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
}

func (l *listLocalOffsets) run(_ *kingpin.ParseContext) error {
	offsetManager := kafka.NewLocalOffsetManager(internal.NewPrinter(l.globalParams.Verbosity, os.Stdout))
	localOffsets, err := offsetManager.ReadTopicOffsets(l.topic, l.environment)
	if err != nil {
		return err
	}
	if len(localOffsets) == 0 {
		return fmt.Errorf("no offset has been stored locally for %s topic in %s", l.topic, l.environment)
	}

	manager, ctx, cancel, err := commands.InitKafkaManager(l.globalParams, l.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	offsets, err := manager.GetTopicOffsets(ctx, l.topic, localOffsets)
	if err != nil {
		return err
	}

	switch l.format {
	case commands.JsonFormat:
		return output.PrintAsJson(offsets.ToJson(), l.style, l.globalParams.EnableColor)
	case commands.TableFormat:
		return l.printAsTable(offsets)
	case commands.TreeFormat:
		return l.printAsList(offsets, false)
	case commands.PlainTextFormat:
		return l.printAsList(offsets, true)
	default:
		return nil
	}
}

func (l *listLocalOffsets) printAsTable(offsets kafka.PartitionOffset) error {
	sortedPartitions := offsets.SortPartitions()
	table := tabular.NewTable(l.globalParams.EnableColor,
		tabular.C("Partition"),
		tabular.C("Latest").MinWidth(10),
		tabular.C("Current").MinWidth(10),
		tabular.C("Lag").MinWidth(10).Humanize().Warn(0, true).FAlign(tabular.AlignCenter),
	)
	table.SetTitle(format.WithCount("Partitions", len(sortedPartitions)))
	var totalLag int64
	for _, partition := range sortedPartitions {
		offsets := offsets[int32(partition)]
		lag := offsets.Lag()
		totalLag += lag
		latest := humanize.Comma(offsets.Latest)
		current := humanize.Comma(offsets.Current)
		table.AddRow(partition, latest, current, lag)
	}
	table.AddFooter(" ", " ", " ", totalLag)
	table.Render()
	return nil
}

func (l *listLocalOffsets) printAsList(offsets kafka.PartitionOffset, plain bool) error {
	partitions := offsets.SortPartitions()
	var totalLag int64
	ls := list.New(plain)
	for _, partition := range partitions {
		offsets := offsets[int32(partition)]
		lag := offsets.Lag()
		totalLag += lag
		ls.AddItemF("P%d", partition)
		ls.Indent()
		ls.AddItemF(" Latest: %s", humanize.Comma(offsets.Latest))
		ls.AddItemF("Current: %s", humanize.Comma(offsets.Current))
		ls.AddItemF("    Lag: %v", format.Warn(lag, l.globalParams.EnableColor && !plain, true))
		ls.UnIndent()
	}
	ls.Render()
	return nil
}
