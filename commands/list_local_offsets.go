package commands

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/gookit/color"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/kafka"
)

type listLocalOffsets struct {
	globalParams *GlobalParameters
	topicsFilter *regexp.Regexp
}

func addListOffsetsSubCommand(parent *kingpin.CmdClause, params *GlobalParameters) {
	cmd := &listLocalOffsets{
		globalParams: params,
	}
	c := parent.Command("list", "Lists the local offsets for different environments.").Action(cmd.run)
	c.Flag("filter", "An optional regular expression to filter the topics by.").RegexpVar(&cmd.topicsFilter)
}

func (c *listLocalOffsets) run(_ *kingpin.ParseContext) error {
	offsetManager := kafka.NewLocalOffsetManager(c.globalParams.Verbosity)
	offsetMap, err := offsetManager.ListLocalOffsets(c.topicsFilter)
	if err != nil {
		return err
	}
	if len(offsetMap) == 0 {
		fmt.Println("No offset has been stored locally.")
	}

	for environment, topicOffsets := range offsetMap {
		if len(topicOffsets) == 0 {
			continue
		}
		sortedTopics := topicOffsets.SortedTopics()

		color.Bold.Print("Environment")
		fmt.Printf(": %s\n", environment)
		table := tablewriter.NewWriter(os.Stdout)
		headers := []string{"Topic", "Partition", "Offset"}
		table.SetHeader(headers)
		table.SetColumnAlignment([]int{tablewriter.ALIGN_LEFT, tablewriter.ALIGN_CENTER, tablewriter.ALIGN_CENTER})
		rows := make([][]string, 0)
		for _, topic := range sortedTopics {
			partitionOffsets := topicOffsets[topic]
			sortedPartitions := partitionOffsets.SortedPartitions()
			for i, partition := range sortedPartitions {
				firstCell := topic
				if i > 0 {
					firstCell = ""
				}
				rows = append(rows, []string{
					firstCell,
					strconv.Itoa(partition),
					strconv.FormatInt(partitionOffsets[int32(partition)], 10),
				})
			}
		}
		table.AppendBulk(rows)
		table.Render()
		fmt.Println()
	}
	return nil
}
