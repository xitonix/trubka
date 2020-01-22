package list

import (
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/kafka"
)

type topics struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	topicFilter  *regexp.Regexp
	format       string
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
	commands.AddFormatFlag(c, &cmd.format)
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
		fmt.Println(commands.GetNotFoundMessage("topic", "topic", c.topicFilter))
		return nil
	}

	sort.Sort(kafka.TopicsByName(topics))

	switch c.format {
	case commands.PlainTextFormat:
		c.printPlainTextOutput(topics)
	case commands.TableFormat:
		c.printTableOutput(topics)
	}
	return nil
}

func (c *topics) printPlainTextOutput(topics []kafka.Topic) {
	var totalPartitions int64
	for _, topic := range topics {
		totalPartitions += int64(topic.NumberOfPartitions)
		fmt.Printf("%s\n", topic)
	}
	fmt.Println("\nTotal\n-----")
	fmt.Printf("Number of topics: %s\n", humanize.Comma(int64(len(topics))))
	fmt.Printf("Number of partitions: %s", humanize.Comma(totalPartitions))
}

func (c *topics) printTableOutput(topics []kafka.Topic) {
	table := commands.InitStaticTable(os.Stdout, map[string]int{
		"Topic":                tablewriter.ALIGN_LEFT,
		"Number of Partitions": tablewriter.ALIGN_CENTER,
		"Replication Factor":   tablewriter.ALIGN_CENTER,
	})

	rows := make([][]string, 0)
	var totalPartitions int64
	for _, topic := range topics {
		np := strconv.FormatInt(int64(topic.NumberOfPartitions), 10)
		rf := strconv.FormatInt(int64(topic.ReplicationFactor), 10)
		totalPartitions += int64(topic.NumberOfPartitions)
		rows = append(rows, []string{
			commands.SpaceIfEmpty(topic.Name),
			commands.SpaceIfEmpty(np),
			commands.SpaceIfEmpty(rf),
		})
	}
	table.AppendBulk(rows)
	table.SetFooter([]string{fmt.Sprintf("Total: %s", humanize.Comma(int64(len(topics)))), humanize.Comma(totalPartitions), " "})
	table.SetFooterAlignment(tablewriter.ALIGN_CENTER)
	table.Render()
}
