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
	envFilter    *regexp.Regexp
	short        bool
}

func addListOffsetsSubCommand(parent *kingpin.CmdClause, params *GlobalParameters) {
	cmd := &listLocalOffsets{
		globalParams: params,
	}
	c := parent.Command("list", "Lists the local offsets for different environments.").Action(cmd.run)
	c.Flag("topic", "An optional regular expression to filter the topics by.").Short('t').RegexpVar(&cmd.topicsFilter)
	c.Flag("environment", "An optional regular expression to filter the environments by.").Short('e').RegexpVar(&cmd.envFilter)
	c.Flag("short", "Enables short output. Offsets wont be printed in this mode.").Short('s').BoolVar(&cmd.short)
}

func (c *listLocalOffsets) run(_ *kingpin.ParseContext) error {
	offsetManager := kafka.NewLocalOffsetManager(c.globalParams.Verbosity)
	offsetMap, err := offsetManager.ListLocalOffsets(c.topicsFilter, c.envFilter)
	if err != nil {
		return err
	}
	if len(offsetMap) == 0 {
		filtered := c.envFilter != nil || c.topicsFilter != nil
		msg := "No offsets have been stored locally."
		if filtered {
			msg += " You might need to tweak the filters."
		}
		color.Warn.Println(msg)
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
		for i, topic := range sortedTopics {
			if c.short {
				fmt.Printf(" %d: %s\n", i+1, topic)
				continue
			}
			partitionOffsets := topicOffsets[topic]
			sortedPartitions := partitionOffsets.SortPartitions()
			for i, partition := range sortedPartitions {
				firstCell := topic
				if i > 0 {
					firstCell = ""
				}
				rows = append(rows, []string{
					firstCell,
					strconv.Itoa(partition),
					strconv.FormatInt(partitionOffsets[int32(partition)].Current, 10),
				})
			}
		}
		if !c.short {
			table.AppendBulk(rows)
			table.Render()
		}
		fmt.Println()
	}
	return nil
}
