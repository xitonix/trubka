package commands

import (
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/olekukonko/tablewriter"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type listBrokers struct {
	globalParams    *GlobalParameters
	kafkaParams     *kafkaParameters
	includeMetadata bool
	topicFilter     *regexp.Regexp

	format string
}

func addListBrokersSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &listBrokers{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("list", "Lists the brokers in the Kafka cluster.").Action(cmd.run)
	c.Flag("meta", "Enables fetching metadata for each broker.").Short('m').BoolVar(&cmd.includeMetadata)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by (valid with --meta only)").
		Short('t').
		RegexpVar(&cmd.topicFilter)
	addFormatFlag(c, &cmd.format)
}

func (c *listBrokers) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := initKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	brokers, err := manager.GetBrokers(ctx, c.includeMetadata)
	if err != nil {
		return fmt.Errorf("failed to list the brokers: %w", err)
	}

	if len(brokers) == 0 {
		return errors.New("No broker found")
	}

	switch c.format {
	case plainTextFormat:
		c.printPlainTextOutput(brokers)
	case tableFormat:
		c.printTableOutput(brokers)
	}
	return nil
}

func (c *listBrokers) printTableOutput(brokers []kafka.Broker) {

	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"ID", "Address"}
	if c.includeMetadata {
		headers = append(headers, "Version", "Topic (No. of Partitions)")
	}
	table.SetHeader(headers)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoWrapText(false)
	for _, broker := range brokers {
		row := []string{strconv.Itoa(broker.ID), broker.Address}
		if c.includeMetadata && len(broker.Meta.Topics) > 0 {
			topics := make([]string, 0)
			for _, topic := range broker.Meta.Topics {
				if c.topicFilter != nil && !c.topicFilter.Match([]byte(topic.Name)) {
					continue
				}
				topics = append(topics, fmt.Sprintf("%s (%d)", topic.Name, topic.NumberOdPartitions))
			}
			if len(topics) > 0 {
				row = append(row,
					strconv.Itoa(broker.Meta.Version),
					strings.Join(topics, "\n"))
			} else {
				row = append(row,
					strconv.Itoa(broker.Meta.Version),
					getNotFoundMessage("topic", "topic", c.topicFilter))
			}
		}
		table.Append(row)
	}
	table.Render()
}

func (c *listBrokers) printPlainTextOutput(brokers []kafka.Broker) {
	for _, broker := range brokers {
		fmt.Printf("%s: %s\n", internal.Bold("Broker", c.globalParams.EnableColor), broker.String())
		if c.includeMetadata && len(broker.Meta.Topics) > 0 {
			topics := make([]string, 0)
			for _, topic := range broker.Meta.Topics {
				if c.topicFilter != nil && !c.topicFilter.Match([]byte(topic.Name)) {
					continue
				}
				topics = append(topics, fmt.Sprintf("  %s (%d)", topic.Name, topic.NumberOdPartitions))
			}
			if len(topics) > 0 {
				fmt.Printf("%s\n\n", internal.Green("TOPICS (No. of Partitions)", c.globalParams.EnableColor))
				fmt.Println(strings.Join(topics, "\n"))
			} else {
				fmt.Println(getNotFoundMessage("topic", "topic", c.topicFilter))
			}
			fmt.Println()
		}
	}
}
