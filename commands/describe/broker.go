package describe

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/kafka"
)

type broker struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	topicsFilter *regexp.Regexp
	address      string
	includeLogs  bool
	format       string
}

func addBrokerSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &broker{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("broker", "Describes a Kafka broker.").Action(cmd.run)
	c.Arg("address", "The broker address.").Required().StringVar(&cmd.address)
	c.Flag("include-logs", "Fetches information about the broker log files.").
		Short('l').
		BoolVar(&cmd.includeLogs)
	c.Flag("topic-filter", "An optional regular expression to filter the aggregated topic logs by. Works with --include-logs only.").
		Short('t').
		RegexpVar(&cmd.topicsFilter)
	commands.AddFormatFlag(c, &cmd.format)
}

func (b *broker) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(b.globalParams, b.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	meta, err := manager.DescribeBroker(ctx, b.address, b.includeLogs, b.topicsFilter)
	if err != nil {
		return err
	}

	sort.Strings(meta.ConsumerGroups)

	switch b.format {
	case commands.PlainTextFormat:
		b.printPlainTextOutput(meta)
	case commands.TableFormat:
		b.printTableOutput(meta)
	}
	return nil
}

func (b *broker) printPlainTextOutput(meta *kafka.BrokerMeta) {
	fmt.Println(commands.Underline("Consumer Groups"))
	for _, group := range meta.ConsumerGroups {
		fmt.Printf(" - %s\n", group)
	}
	fmt.Println()

	if b.includeLogs && len(meta.Logs) != 0 {
		b.printLogsPlain(meta.Logs)
	}
}

func (b *broker) printTableOutput(meta *kafka.BrokerMeta) {
	table := commands.InitStaticTable(os.Stdout, map[string]int{
		"Consumer Groups": tablewriter.ALIGN_LEFT,
	})
	for _, group := range meta.ConsumerGroups {
		table.Append([]string{commands.SpaceIfEmpty(group)})
	}
	table.Render()

	if b.includeLogs && len(meta.Logs) != 0 {
		b.printLogsTable(meta.Logs)
	}
}

func (b *broker) printLogsTable(logs []*kafka.LogFile) {
	for _, logFile := range logs {
		fmt.Printf("\nLog File Path: %s\n", logFile.Path)
		sorted := logFile.SortByPermanentSize()
		if len(sorted) == 0 {
			msg := commands.GetNotFoundMessage("topic log", "topic", b.topicsFilter)
			fmt.Println(msg)
			return
		}
		table := commands.InitStaticTable(os.Stdout, map[string]int{
			"Topic":          tablewriter.ALIGN_LEFT,
			"Permanent Logs": tablewriter.ALIGN_CENTER,
			"Temporary Logs": tablewriter.ALIGN_CENTER,
		})
		rows := make([][]string, 0)

		for _, tLogs := range sorted {
			row := []string{
				commands.SpaceIfEmpty(tLogs.Topic),
				commands.SpaceIfEmpty(humanize.Bytes(tLogs.Permanent)),
				commands.SpaceIfEmpty(humanize.Bytes(tLogs.Temporary)),
			}
			rows = append(rows, row)
		}
		table.AppendBulk(rows)
		table.Render()
	}
}

func (b *broker) printLogsPlain(logs []*kafka.LogFile) {
	for _, logFile := range logs {
		title := fmt.Sprintf("\nPath: %s", logFile.Path)
		fmt.Printf("%s\n", commands.Underline(title))
		sorted := logFile.SortByPermanentSize()
		if len(sorted) == 0 {
			msg := commands.GetNotFoundMessage("topic log", "topic", b.topicsFilter)
			fmt.Println(msg)
			return
		}
		for _, tLogs := range sorted {
			fmt.Printf(" - %s: PERM %s, TEMP %s\n",
				tLogs.Topic,
				humanize.Bytes(tLogs.Permanent),
				humanize.Bytes(tLogs.Temporary))
		}
	}
}
