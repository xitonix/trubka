package commands

import (
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type listTopics struct {
	kafkaParams  *kafkaParameters
	globalParams *GlobalParameters

	topicFilter    *regexp.Regexp
	includeOffsets bool
	environment    string
	format         string
}

func addListTopicsSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &listTopics{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("list", "Loads the existing topics from the server.").Action(cmd.run)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").Short('t').RegexpVar(&cmd.topicFilter)
	c.Flag("partitions", "If enabled, the partition offset data will be retrieved too.").Short('p').BoolVar(&cmd.includeOffsets)
	c.Flag("environment", "The environment to load the local offsets for (if any).").Short('e').StringVar(&cmd.environment)
	addFormatFlag(c, &cmd.format)
}

func (c *listTopics) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := initKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	topics, err := manager.GetTopics(ctx, c.topicFilter, c.includeOffsets, c.environment)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		fmt.Println(getNotFoundMessage("topic", "topic", c.topicFilter))
		return nil
	}

	switch c.format {
	case plainTextFormat:
		c.printPlainTextOutput(topics)
	case tableFormat:
		c.printTableOutput(topics)
	}
	return nil
}

func (c *listTopics) printPlainTextOutput(tpo kafka.TopicPartitionOffset) {
	sortedTopics := tpo.SortedTopics()
	for _, topic := range sortedTopics {
		fmt.Printf("%s: %s\n", internal.Bold("Topic"), topic)
		partitions := tpo[topic]
		if !c.includeOffsets {
			continue
		}
		keys := partitions.SortPartitions()
		fmt.Println()
		for _, partition := range keys {
			offset := partitions[int32(partition)]
			msg := fmt.Sprintf("  Partition %2d: ", partition)
			if offset.Current >= 0 {
				msg += fmt.Sprintf(" Local Offset %d out of %d", offset.Current, offset.Latest)
				lag := offset.Lag()
				if lag > 0 {
					msg += fmt.Sprintf(" (Lag: %s)", highlightLag(lag))
				}
			} else {
				msg += fmt.Sprintf("%d", offset.Latest)
			}
			fmt.Println(msg)
		}
		fmt.Println()
	}
}

func (c *listTopics) printTableOutput(tpo kafka.TopicPartitionOffset) {
	sortedTopics := tpo.SortedTopics()

	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"Topic"}
	if c.includeOffsets {
		headers = append(headers, "Partition", "Latest Offset", "Local Offset", "Lag")
	}
	table.SetHeader(headers)
	table.SetColumnAlignment([]int{
		tablewriter.ALIGN_LEFT,
		tablewriter.ALIGN_CENTER,
		tablewriter.ALIGN_CENTER,
		tablewriter.ALIGN_CENTER,
		tablewriter.ALIGN_CENTER,
	})
	for _, topic := range sortedTopics {
		partitions := tpo[topic]
		row := []string{topic}
		if !c.includeOffsets {
			table.Append(row)
			continue
		}
		keys := partitions.SortPartitions()
		rows := make([][]string, 0)
		for i, partition := range keys {
			firstCell := topic
			if i > 0 {
				firstCell = ""
			}
			op := partitions[int32(partition)]
			lagStr := "-"
			if op.Current >= 0 {
				lagStr = highlightLag(op.Lag())
			}
			rows = append(rows, []string{firstCell, strconv.Itoa(partition), op.String(true), op.String(false), lagStr})
		}
		table.AppendBulk(rows)

	}
	table.Render()
}
