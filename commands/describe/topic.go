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
	"github.com/xitonix/trubka/kafka"
)

type topic struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	topic        string
	format       string
}

func addTopicSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &topic{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("topic", "Describes a Kafka topic.").Action(cmd.run)
	c.Arg("topic", "The topic to describe.").Required().StringVar(&cmd.topic)
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

	meta, err := manager.DescribeTopic(ctx, t.topic)
	if err != nil {
		return err
	}

	sort.Sort(kafka.PartitionMetaById(meta))

	switch t.format {
	case commands.PlainTextFormat:
		t.printPlainTextOutput(meta)
	case commands.TableFormat:
		t.printTableOutput(meta)
	}
	return nil
}

func (t *topic) printPlainTextOutput(meta []*kafka.PartitionMeta) {
	for _, pm := range meta {
		fmt.Printf("P%d: Leader: %s\n - ISRs: %s\n - Replicas: %s\n - Offline Replicas: %s\n\n",
			pm.Id,
			pm.Leader.Host,
			t.brokersToLine(pm.ISRs...),
			t.brokersToLine(pm.Replicas...),
			t.brokersToLine(pm.OfflineReplicas...))
	}
}

func (t *topic) printTableOutput(meta []*kafka.PartitionMeta) {
	table := commands.InitStaticTable(os.Stdout,
		commands.H("Partition", tablewriter.ALIGN_CENTER),
		commands.H("Leader", tablewriter.ALIGN_LEFT),
		commands.H("Replicas", tablewriter.ALIGN_LEFT),
		commands.H("Offline Replicas", tablewriter.ALIGN_LEFT),
		commands.H("ISRs", tablewriter.ALIGN_LEFT),
	)

	for _, pm := range meta {
		partition := strconv.FormatInt(int64(pm.Id), 10)
		table.Append([]string{
			partition,
			commands.SpaceIfEmpty(t.brokersToList(pm.Leader)),
			commands.SpaceIfEmpty(t.brokersToList(pm.Replicas...)),
			commands.SpaceIfEmpty(t.brokersToList(pm.OfflineReplicas...)),
			commands.SpaceIfEmpty(t.brokersToList(pm.ISRs...)),
		})
	}
	table.SetFooter([]string{fmt.Sprintf("Total: %d", len(meta)), " ", " ", " ", " "})
	table.SetAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}

func (*topic) brokersToList(brokers ...kafka.Broker) string {
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

func (*topic) brokersToLine(brokers ...kafka.Broker) string {
	result := make([]string, len(brokers))
	for i, b := range brokers {
		result[i] = b.Host
	}
	return strings.Join(result, ", ")
}
