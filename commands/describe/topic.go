package describe

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/kafka"
)

type topic struct {
	kafkaParams    *commands.KafkaParameters
	globalParams   *commands.GlobalParameters
	topic          string
	loadConfigs    bool
	includeOffsets bool
	format         string
}

func addTopicSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &topic{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("topic", "Describes a Kafka topic.").Action(cmd.run)
	c.Arg("topic", "The topic to describe.").Required().StringVar(&cmd.topic)
	c.Flag("load-config", "Loads the topic's configurations from the server.").
		NoEnvar().
		Short('C').BoolVar(&cmd.loadConfigs)
	c.Flag("include-offsets", "Queries the server to read the latest available offset of each partition.").
		NoEnvar().
		Short('o').BoolVar(&cmd.includeOffsets)
	commands.AddFormatFlag(c, &cmd.format)
}

func (t *topic) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(t.globalParams, t.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	meta, err := manager.DescribeTopic(ctx, t.topic, t.loadConfigs, t.includeOffsets)
	if err != nil {
		return err
	}

	sort.Sort(kafka.PartitionMetaById(meta.Partitions))
	if t.loadConfigs {
		sort.Sort(kafka.ConfigEntriesByName(meta.ConfigEntries))
	}

	switch t.format {
	case commands.PlainTextFormat:
		t.printPlainTextOutput(meta)
	case commands.TableFormat:
		t.printTableOutput(meta)
	}
	return nil
}

func (t *topic) printPlainTextOutput(meta *kafka.TopicMetadata) {
	var totalOffsets int64
	output.UnderlineWithCount("Partitions", len(meta.Partitions))
	for _, pm := range meta.Partitions {
		var offset string
		if t.includeOffsets {
			offset = fmt.Sprintf("\n - Offset: %s", humanize.Comma(pm.Offset))
			totalOffsets += pm.Offset
		}
		fmt.Printf("P%d: %s\n - Leader: %s\n - ISRs: %s\n - Replicas: %s",
			pm.Id,
			offset,
			pm.Leader.Host,
			t.brokersToLine(pm.ISRs...),
			t.brokersToLine(pm.Replicas...))

		if len(pm.OfflineReplicas) > 0 {
			fmt.Printf("\n - Offline Replicas: %s", t.brokersToLine(pm.OfflineReplicas...))
		}
		fmt.Print("\n\n")
	}

	if t.includeOffsets {
		fmt.Println(output.Underline("Total Offsets"))
		fmt.Println(humanize.Comma(totalOffsets))
	}

	if t.loadConfigs {
		commands.PrintConfigPlain(meta.ConfigEntries)
	}
}

func (t *topic) printTableOutput(meta *kafka.TopicMetadata) {
	var table *tablewriter.Table
	if t.includeOffsets {
		table = output.InitStaticTable(os.Stdout,
			output.H("Partition", tablewriter.ALIGN_CENTER),
			output.H("Offset", tablewriter.ALIGN_CENTER),
			output.H("Leader", tablewriter.ALIGN_LEFT),
			output.H("Replicas", tablewriter.ALIGN_LEFT),
			output.H("Offline Replicas", tablewriter.ALIGN_LEFT),
			output.H("ISRs", tablewriter.ALIGN_LEFT),
		)
	} else {
		table = output.InitStaticTable(os.Stdout,
			output.H("Partition", tablewriter.ALIGN_CENTER),
			output.H("Leader", tablewriter.ALIGN_LEFT),
			output.H("Replicas", tablewriter.ALIGN_LEFT),
			output.H("Offline Replicas", tablewriter.ALIGN_LEFT),
			output.H("ISRs", tablewriter.ALIGN_LEFT),
		)
	}
	output.WithCount("Partitions", len(meta.Partitions))
	var totalOffsets int64
	for _, pm := range meta.Partitions {
		partition := strconv.FormatInt(int64(pm.Id), 10)
		row := []string{partition}

		if t.includeOffsets {
			row = append(row, humanize.Comma(pm.Offset))
			totalOffsets += pm.Offset
		}
		row = append(row,
			output.SpaceIfEmpty(t.brokersToList(pm.Leader)),
			output.SpaceIfEmpty(t.brokersToList(pm.Replicas...)),
			output.SpaceIfEmpty(t.brokersToList(pm.OfflineReplicas...)),
			output.SpaceIfEmpty(t.brokersToList(pm.ISRs...)),
		)
		table.Append(row)
	}

	footer := []string{fmt.Sprintf("Total: %d", len(meta.Partitions))}
	if t.includeOffsets {
		footer = append(footer, humanize.Comma(totalOffsets))
	}
	footer = append(footer, " ", " ", " ", " ")
	table.SetFooter(footer)
	table.SetFooterAlignment(tablewriter.ALIGN_CENTER)
	table.Render()

	if t.loadConfigs {
		commands.PrintConfigTable(meta.ConfigEntries)
	}
}

func (*topic) brokersToList(brokers ...*kafka.Broker) string {
	if len(brokers) == 1 {
		return brokers[0].Host
	}
	var buf bytes.Buffer
	for i, b := range brokers {
		buf.WriteString(fmt.Sprintf("%s", b.Host))
		if i < len(brokers)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func (*topic) brokersToLine(brokers ...*kafka.Broker) string {
	result := make([]string, len(brokers))
	for i, b := range brokers {
		result[i] = b.Host
	}
	return strings.Join(result, ", ")
}
