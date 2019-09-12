package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"

	"github.com/gookit/color"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/kafka"
)

type topics struct {
	kafkaParams  *kafkaParameters
	globalParams *GlobalParameters

	filter         *regexp.Regexp
	includeOffsets bool
	environment    string
	format         string
}

func addTopicsSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &topics{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("topics", "Loads the existing topics from the server.").Action(cmd.run)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").Short('t').RegexpVar(&cmd.filter)
	c.Flag("partitions", "If enabled, the partition offset data will be retrieved too.").Short('p').BoolVar(&cmd.includeOffsets)
	c.Flag("environment", "The environment to load the local offsets for (if any).").Short('e').StringVar(&cmd.environment)
	c.Flag("format", "Sets the output format.").
		Default(tableFormat).
		Short('f').
		EnumVar(&cmd.format, plainTextFormat, tableFormat)
}

func (c *topics) run(_ *kingpin.ParseContext) error {
	manager, err := kafka.NewManager(c.kafkaParams.brokers,
		c.globalParams.Verbosity,
		kafka.WithClusterVersion(c.kafkaParams.version),
		kafka.WithTLS(c.kafkaParams.tls),
		kafka.WithSASL(c.kafkaParams.saslMechanism, c.kafkaParams.saslUsername, c.kafkaParams.saslPassword))

	if err != nil {
		return err
	}

	defer func() {
		if err := manager.Close(); err != nil {
			color.Error.Printf("Failed to close the Kafka client: %s", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		cancel()
	}()

	topics, err := manager.GetTopics(ctx, c.filter, c.includeOffsets, c.environment)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		fmt.Println("No topics found!")
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

func (c *topics) printPlainTextOutput(tpo kafka.TopicPartitionOffset) {
	sortedTopics := tpo.SortedTopics()
	for _, topic := range sortedTopics {
		color.Bold.Print("Topic: ")
		fmt.Println(topic)
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

func (c *topics) printTableOutput(tpo kafka.TopicPartitionOffset) {
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
