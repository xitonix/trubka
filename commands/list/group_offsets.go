package list

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/kafka"
)

type groupOffset struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	group        string
	topicFilter  *regexp.Regexp
	format       string
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
	commands.AddFormatFlag(c, &cmd.format)
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
		fmt.Println(internal.GetNotFoundMessage("topic", "topic", g.topicFilter))
		return nil
	}

	switch g.format {
	case commands.PlainTextFormat:
		g.printPlainTextOutput(topics)
	case commands.TableFormat:
		g.printTableOutput(topics)
	}
	return nil
}

func (g *groupOffset) printTableOutput(topics kafka.TopicPartitionOffset) {
	for topic, partitionOffsets := range topics {
		fmt.Printf("%s: %s\n",
			internal.Bold("TOPIC", g.globalParams.EnableColor),
			internal.Bold(topic, g.globalParams.EnableColor))

		if len(partitionOffsets) > 0 {
			table := output.InitStaticTable(os.Stdout,
				output.H("Partition", tablewriter.ALIGN_CENTER),
				output.H("Latest", tablewriter.ALIGN_CENTER),
				output.H("Current", tablewriter.ALIGN_CENTER),
				output.H("Lag", tablewriter.ALIGN_CENTER),
			)
			table.SetColMinWidth(0, 10)
			table.SetColMinWidth(1, 10)
			table.SetColMinWidth(2, 10)
			table.SetColMinWidth(3, 10)
			partitions := partitionOffsets.SortPartitions()
			var totalLag int64
			for _, partition := range partitions {
				offsets := partitionOffsets[int32(partition)]
				lag := offsets.Lag()
				totalLag += offsets.Lag()
				latest := humanize.Comma(offsets.Latest)
				current := humanize.Comma(offsets.Current)
				part := strconv.FormatInt(int64(partition), 10)
				table.Append([]string{part, latest, current, fmt.Sprint(highlightLag(lag, g.globalParams.EnableColor))})
			}
			table.SetFooter([]string{" ", " ", " ", humanize.Comma(totalLag)})
			table.SetFooterAlignment(tablewriter.ALIGN_CENTER)
			table.Render()
		}
	}
}

func (g *groupOffset) printPlainTextOutput(topics kafka.TopicPartitionOffset) {
	for topic, partitionOffsets := range topics {
		fmt.Printf("%s\n", internal.Bold(topic, g.globalParams.EnableColor))
		if len(partitionOffsets) > 0 {
			fmt.Printf("\n")
			var totalLag int64
			partitions := partitionOffsets.SortPartitions()
			for _, partition := range partitions {
				offsets := partitionOffsets[int32(partition)]
				lag := offsets.Lag()
				totalLag += offsets.Lag()
				fmt.Printf("   Partition %2d: %d out of %d (Lag: %s) \n", partition, offsets.Current, offsets.Latest,
					highlightLag(lag, g.globalParams.EnableColor))
			}
			fmt.Printf("   -----------------\n   Total Lag: %s\n\n", humanize.Comma(totalLag))
		}
	}
}
