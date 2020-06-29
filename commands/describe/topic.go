package describe

import (
	"bytes"
	"fmt"
	"sort"
	"strings"

	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
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

	if len(meta.Partitions) == 0 {
		return fmt.Errorf("topic %s not found", t.topic)
	}

	sort.Sort(kafka.PartitionMetaById(meta.Partitions))
	if t.loadConfigs {
		sort.Sort(kafka.ConfigEntriesByName(meta.ConfigEntries))
	}

	switch t.format {
	case commands.ListFormat:
		t.printListOutput(meta, false)
	case commands.TableFormat:
		t.printTableOutput(meta)
	case commands.PlainTextFormat:
		t.printListOutput(meta, true)
	}
	return nil
}

func (t *topic) printListOutput(meta *kafka.TopicMetadata, plain bool) {
	var totalOffsets int64
	b := list.New(plain)
	b.AsTree()
	b.SetTitle(format.WithCount("Partitions", len(meta.Partitions)))
	for _, pm := range meta.Partitions {
		b.AddItemF("P%d", pm.Id)
		b.Intend()
		if t.includeOffsets {
			b.AddItemF("Offset: %s", humanize.Comma(pm.Offset))
			totalOffsets += pm.Offset
		}
		b.Intend()
		b.AddItemF("Leader: %s", pm.Leader.MarkedHostName())
		b.AddItemF("ISRs: %s", t.brokersToLine(pm.ISRs...))
		b.AddItemF("Replicas: %s", t.brokersToLine(pm.Replicas...))
		if len(pm.OfflineReplicas) > 0 {
			b.AddItemF("Offline Replicas: %s", t.brokersToLine(pm.OfflineReplicas...))
		}
		b.UnIntend()
		b.UnIntend()
	}
	b.SetCaption(kafka.ControllerBrokerLabel + " CONTROLLER NODES")
	b.Render()

	if t.includeOffsets {
		output.NewLines(1)
		fmt.Println(format.Underline("Total Offsets"))
		fmt.Println(humanize.Comma(totalOffsets))
	}

	if t.loadConfigs {
		output.NewLines(2)
		commands.PrintConfigList(meta.ConfigEntries, plain)
	}
}

func (t *topic) printTableOutput(meta *kafka.TopicMetadata) {
	table := tabular.NewTable(t.globalParams.EnableColor,
		tabular.C("Partition"),
		tabular.C("Offset").FAlign(tabular.AlignCenter),
		tabular.C("Leader").Align(tabular.AlignLeft),
		tabular.C("Replicas").Align(tabular.AlignLeft),
		tabular.C("Offline Replicas").Align(tabular.AlignLeft),
		tabular.C("ISRs").Align(tabular.AlignLeft),
	)
	table.SetTitle(format.WithCount("Partitions", len(meta.Partitions)))
	var totalOffsets int64
	for _, pm := range meta.Partitions {
		offset := "-"
		if t.includeOffsets {
			offset = humanize.Comma(pm.Offset)
			totalOffsets += pm.Offset
		}
		table.AddRow(
			pm.Id,
			offset,
			format.SpaceIfEmpty(pm.Leader.MarkedHostName()),
			format.SpaceIfEmpty(t.brokersToList(pm.Replicas...)),
			format.SpaceIfEmpty(t.brokersToList(pm.OfflineReplicas...)),
			format.SpaceIfEmpty(t.brokersToList(pm.ISRs...)),
		)
	}

	total := " "
	if t.includeOffsets {
		total = humanize.Comma(totalOffsets)
	}
	table.AddFooter(fmt.Sprintf("Total: %d", len(meta.Partitions)), total, " ", " ", " ", " ")
	table.SetCaption(kafka.ControllerBrokerLabel + " CONTROLLER NODES")
	table.Render()

	if t.loadConfigs {
		commands.PrintConfigTable(meta.ConfigEntries)
	}
}

func (t *topic) brokersToList(brokers ...*kafka.Broker) string {
	if len(brokers) == 1 {
		return brokers[0].Host
	}
	var buf bytes.Buffer
	for i, b := range brokers {
		buf.WriteString(b.MarkedHostName())
		if i < len(brokers)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func (t *topic) brokersToLine(brokers ...*kafka.Broker) string {
	result := make([]string, len(brokers))
	for i, b := range brokers {
		result[i] = b.MarkedHostName()
	}
	return strings.Join(result, ", ")
}
