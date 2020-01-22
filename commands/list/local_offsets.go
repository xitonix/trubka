package list

import (
	"fmt"
	"os"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
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
		fmt.Printf("no offset has been stored locally for %s topic in %s", l.topic, l.environment)
		return nil
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
	case commands.PlainTextFormat:
		l.printPlainTextOutput(offsets)
	case commands.TableFormat:
		l.printTableOutput(offsets)
	}
	return nil
}

func (l *listLocalOffsets) printTableOutput(offsets kafka.PartitionOffset) {
	sortedPartitions := offsets.SortPartitions()

	table := commands.InitStaticTable(os.Stdout, map[string]int{
		"Partition": tablewriter.ALIGN_CENTER,
		"Latest":    tablewriter.ALIGN_CENTER,
		"Current":   tablewriter.ALIGN_CENTER,
		"Lag":       tablewriter.ALIGN_CENTER,
	})
	table.SetColMinWidth(1, 10)
	table.SetColMinWidth(2, 10)
	table.SetColMinWidth(3, 10)
	for _, partition := range sortedPartitions {
		offsets := offsets[int32(partition)]
		latest := humanize.Comma(offsets.Latest)
		current := humanize.Comma(offsets.Current)
		part := strconv.FormatInt(int64(partition), 10)
		table.Append([]string{part, latest, current, fmt.Sprint(commands.HighlightLag(offsets.Lag(), l.globalParams.EnableColor))})
	}
	table.Render()
}

func (l *listLocalOffsets) printPlainTextOutput(offsets kafka.PartitionOffset) {
	partitions := offsets.SortPartitions()
	for _, partition := range partitions {
		offsets := offsets[int32(partition)]
		fmt.Printf("   Partition %2d: %d out of %d (Lag: %s) \n", partition, offsets.Current, offsets.Latest,
			commands.HighlightLag(offsets.Lag(), l.globalParams.EnableColor))
	}
}
