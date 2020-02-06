package describe

import (
	"bytes"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

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
		Short('C').BoolVar(&cmd.loadConfigs)
	c.Flag("include-offsets", "Reads the latest available offset of each partition from the server.").
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

	meta, err := manager.DescribeTopic(ctx, t.topic, t.loadConfigs)
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
	for _, pm := range meta.Partitions {
		fmt.Printf("P%d: Leader: %s\n - ISRs: %s\n - Replicas: %s\n - Offline Replicas: %s\n\n",
			pm.Id,
			pm.Leader.Host,
			t.brokersToLine(pm.ISRs...),
			t.brokersToLine(pm.Replicas...),
			t.brokersToLine(pm.OfflineReplicas...))
	}

	if t.loadConfigs {
		commands.PrintConfigPlain(meta.ConfigEntries)
	}
}

func (t *topic) printTableOutput(meta *kafka.TopicMetadata) {
	table := output.InitStaticTable(os.Stdout,
		output.H("Partition", tablewriter.ALIGN_CENTER),
		output.H("Leader", tablewriter.ALIGN_LEFT),
		output.H("Replicas", tablewriter.ALIGN_LEFT),
		output.H("Offline Replicas", tablewriter.ALIGN_LEFT),
		output.H("ISRs", tablewriter.ALIGN_LEFT),
	)

	for _, pm := range meta.Partitions {
		partition := strconv.FormatInt(int64(pm.Id), 10)
		table.Append([]string{
			partition,
			output.SpaceIfEmpty(t.brokersToList(pm.Leader)),
			output.SpaceIfEmpty(t.brokersToList(pm.Replicas...)),
			output.SpaceIfEmpty(t.brokersToList(pm.OfflineReplicas...)),
			output.SpaceIfEmpty(t.brokersToList(pm.ISRs...)),
		})
	}
	table.SetFooter([]string{fmt.Sprintf("Total: %d", len(meta.Partitions)), " ", " ", " ", " "})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
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
