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
	style          string
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
		Short('c').BoolVar(&cmd.loadConfigs)
	c.Flag("include-offsets", "Queries the server to read the latest available offset of each partition.").
		NoEnvar().
		Short('o').BoolVar(&cmd.includeOffsets)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
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
	case commands.JsonFormat:
		return output.PrintAsJson(meta, t.style, t.globalParams.EnableColor)
	case commands.TableFormat:
		return t.printAsTable(meta)
	case commands.TreeFormat:
		return t.printAsList(meta, false)
	case commands.PlainTextFormat:
		return t.printAsList(meta, true)
	default:
		return nil
	}
}

func (t *topic) printAsList(meta *kafka.TopicMetadata, plain bool) error {
	var totalOffsets int64
	l := list.New(plain)
	l.AddItem("Partitions")
	l.Indent()
	for _, pm := range meta.Partitions {
		l.AddItemF("%d", pm.Id)
		l.Indent()
		if t.includeOffsets {
			l.AddItemF("Offset: %s", humanize.Comma(pm.Offset))
			totalOffsets += pm.Offset
		}
		l.AddItemF("Leader: %s", pm.Leader.String())
		l.AddItemF("ISRs: %s", t.brokersToLine(pm.ISRs...))
		l.AddItemF("Replicas: %s", t.brokersToLine(pm.Replicas...))
		if len(pm.OfflineReplicas) > 0 {
			l.AddItemF("Offline Replicas: %s", t.brokersToLine(pm.OfflineReplicas...))
		}
		l.UnIndent()
	}
	l.UnIndent()
	if t.loadConfigs {
		commands.PrintConfigList(l, meta.ConfigEntries, plain)
	}
	l.Render()

	return nil
}

func (t *topic) printAsTable(meta *kafka.TopicMetadata) error {
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

	return nil
}

func (t *topic) brokersToList(brokers ...*kafka.Broker) string {
	if len(brokers) == 1 {
		return brokers[0].Host
	}
	var buf bytes.Buffer
	for i, b := range brokers {
		buf.WriteString(b.String())
		if i < len(brokers)-1 {
			buf.WriteString("\n")
		}
	}
	return buf.String()
}

func (t *topic) brokersToLine(brokers ...*kafka.Broker) string {
	result := make([]string, len(brokers))
	for i, b := range brokers {
		result[i] = b.String()
	}
	return strings.Join(result, ", ")
}
