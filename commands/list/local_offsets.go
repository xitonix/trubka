package list

import (
	"fmt"

	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
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
}

func addLocalOffsetsSubCommand(parent *kingpin.CmdClause, params *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &listLocalOffsets{
		globalParams: params,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("local-offsets", "Lists the locally stored offsets of the given topic and environment.").Action(cmd.run)
	c.Arg("topic", "The topic to loads the local offsets of.").Required().StringVar(&cmd.topic)
	c.Arg("environment", "The environment to load the topic offset from.").Required().StringVar(&cmd.environment)
	commands.AddFormatFlag(c, &cmd.format)
}

func (l *listLocalOffsets) run(_ *kingpin.ParseContext) error {
	offsetManager := kafka.NewLocalOffsetManager(l.globalParams.Verbosity)
	localOffsets, err := offsetManager.ReadLocalTopicOffsets(l.topic, l.environment)
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
	case commands.ListFormat:
		l.printListOutput(offsets, false)
	case commands.TableFormat:
		l.printTableOutput(offsets)
	case commands.PlainTextFormat:
		l.printListOutput(offsets, true)
	}
	return nil
}

func (l *listLocalOffsets) printTableOutput(offsets kafka.PartitionOffset) {
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
}

func (l *listLocalOffsets) printListOutput(offsets kafka.PartitionOffset, plain bool) {
	partitions := offsets.SortPartitions()
	var totalLag int64
	b := list.New(plain)
	b.AsTree()
	b.SetTitle(format.WithCount("Partitions", len(partitions)))
	for _, partition := range partitions {
		offsets := offsets[int32(partition)]
		lag := offsets.Lag()
		totalLag += lag
		b.AddItemF("P%d", partition)
		b.Intend()
		b.AddItemF(" Latest: %s", humanize.Comma(offsets.Latest))
		b.AddItemF("Current: %s", humanize.Comma(offsets.Current))
		b.AddItemF("    Lag: %v", format.Warn(lag, l.globalParams.EnableColor, true))
		b.UnIntend()
	}
	b.Render()
	fmt.Printf("\nTotal Lag: %v", format.Warn(totalLag, l.globalParams.EnableColor, true))
}
