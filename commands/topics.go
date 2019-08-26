package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"sort"
	"strconv"
	"syscall"

	"github.com/gookit/color"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/kafka"
)

type topics struct {
	params         *Parameters
	filter         *regexp.Regexp
	includeOffsets bool
	environment    string
}

func addTopicsSubCommand(parent *kingpin.CmdClause, params *Parameters) {
	cmd := &topics{
		params: params,
	}
	c := parent.Command("topics", "Loads the existing topics from the server.").Action(cmd.run)
	c.Flag("filter", "An optional regular expression to filter the topics by.").RegexpVar(&cmd.filter)
	c.Flag("partitions", "If enabled, the partition offset data will be retrieved too.").BoolVar(&cmd.includeOffsets)
	c.Flag("environment", "The environment to load the local offsets for (if any).").StringVar(&cmd.environment)
}

func (c *topics) run(_ *kingpin.ParseContext) error {
	manager, err := kafka.NewManager(c.params.Brokers,
		c.params.Verbosity,
		kafka.WithClusterVersion(c.params.KafkaVersion),
		kafka.WithTLS(c.params.TLS),
		kafka.WithClusterVersion(c.params.KafkaVersion),
		kafka.WithTLS(c.params.TLS),
		kafka.WithSASL(c.params.SASLMechanism, c.params.SASLUsername, c.params.SASLPassword))

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

	sortedTopics := sortTopics(topics)

	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"Topic"}
	if c.includeOffsets {
		headers = append(headers, "Partition", "Latest Offset", "Local Offset")
	}
	table.SetHeader(headers)
	table.SetAlignment(tablewriter.ALIGN_CENTER)
	for _, topic := range sortedTopics {
		partitions := topics[topic]
		row := []string{topic}
		if !c.includeOffsets {
			table.Append(row)
			continue
		}
		keys := sortPartitions(partitions)
		rows := make([][]string, 0)
		for i, partition := range keys {
			firstCell := topic
			if i > 0 {
				firstCell = ""
			}
			op := partitions[int32(partition)]
			local := op.LocalString()
			if op.Local >= 0 && op.Local < op.Remote {
				local = color.Warn.Sprint(local)
			}
			rows = append(rows, []string{firstCell, strconv.Itoa(partition), op.RemoteString(), local})
		}
		table.AppendBulk(rows)

	}
	table.Render()
	return nil
}

func sortPartitions(partitions kafka.PartitionsOffsetPair) []int {
	sorted := make([]int, 0)
	if len(partitions) == 0 {
		return sorted
	}
	for partition := range partitions {
		sorted = append(sorted, int(partition))
	}
	sort.Ints(sorted)
	return sorted
}

func sortTopics(topics map[string]kafka.PartitionsOffsetPair) []string {
	sorted := make([]string, 0)
	if len(topics) == 0 {
		return sorted
	}
	for topic := range topics {
		sorted = append(sorted, topic)
	}
	sort.Strings(sorted)
	return sorted
}
