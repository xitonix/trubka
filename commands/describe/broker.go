package describe

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
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/kafka"
)

type broker struct {
	kafkaParams        *commands.KafkaParameters
	globalParams       *commands.GlobalParameters
	topicsFilter       *regexp.Regexp
	identifier         string
	includeLogs        bool
	includeAPIVersions bool
	format             string
}

func addBrokerSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &broker{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("broker", "Describes a Kafka broker.").Action(cmd.run)
	c.Arg("broker", "The broker address or Id.").Required().StringVar(&cmd.identifier)
	c.Flag("include-logs", "Fetches information about the broker log files.").
		Short('l').
		BoolVar(&cmd.includeLogs)
	c.Flag("include-api-versions", "Fetches the API versions supported by the broker.").
		Short('a').
		BoolVar(&cmd.includeAPIVersions)
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

	meta, err := manager.DescribeBroker(ctx, b.identifier, b.includeLogs, b.includeAPIVersions, b.topicsFilter)
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

func (b *broker) printHeader(isController bool) {
	c := internal.BoolToString(isController)
	fmt.Printf("Controller Node: %s\n",
		internal.Bold(c, isController && b.globalParams.EnableColor))
}
func (b *broker) printPlainTextOutput(meta *kafka.BrokerMeta) {
	b.printHeader(meta.IsController)
	fmt.Printf("\n%s\n", output.UnderlineWithCount("Consumer Group", len(meta.ConsumerGroups)))
	for _, group := range meta.ConsumerGroups {
		fmt.Printf(" - %s\n", group)
	}
	fmt.Println()

	if b.includeLogs && len(meta.Logs) != 0 {
		b.printLogsPlain(meta.Logs)
	}

	if b.includeAPIVersions && len(meta.APIs) != 0 {
		sort.Sort(kafka.APIByCode(meta.APIs))
		b.printAPIPlain(meta.APIs)
	}
}

func (b *broker) printTableOutput(meta *kafka.BrokerMeta) {
	b.printHeader(meta.IsController)
	table := output.InitStaticTable(os.Stdout, output.H("Consumer Groups", tablewriter.ALIGN_LEFT))
	for _, group := range meta.ConsumerGroups {
		table.Append([]string{output.SpaceIfEmpty(group)})
	}
	table.SetFooter([]string{fmt.Sprintf("Total: %d", len(meta.ConsumerGroups))})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()

	if b.includeLogs && len(meta.Logs) != 0 {
		b.printLogsTable(meta.Logs)
	}

	if b.includeAPIVersions && len(meta.APIs) != 0 {
		sort.Sort(kafka.APIByCode(meta.APIs))
		b.printAPITable(meta.APIs)
	}
}

func (b *broker) printLogsTable(logs []*kafka.LogFile) {
	for _, logFile := range logs {
		fmt.Printf("\nLog File Path: %s\n", logFile.Path)
		sorted := logFile.SortByPermanentSize()
		if len(sorted) == 0 {
			msg := internal.GetNotFoundMessage("topic log", "topic", b.topicsFilter)
			fmt.Println(msg)
			return
		}
		table := output.InitStaticTable(os.Stdout,
			output.H("Topic", tablewriter.ALIGN_LEFT),
			output.H("Permanent Logs", tablewriter.ALIGN_CENTER),
			output.H("Temporary Logs", tablewriter.ALIGN_CENTER),
		)
		rows := make([][]string, 0)

		for _, tLogs := range sorted {
			row := []string{
				output.SpaceIfEmpty(tLogs.Topic),
				output.SpaceIfEmpty(humanize.Bytes(tLogs.Permanent)),
				output.SpaceIfEmpty(humanize.Bytes(tLogs.Temporary)),
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
		fmt.Printf("%s\n", output.Underline(title))
		sorted := logFile.SortByPermanentSize()
		if len(sorted) == 0 {
			msg := internal.GetNotFoundMessage("topic log", "topic", b.topicsFilter)
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

func (b *broker) printAPITable(apis []*kafka.API) {
	fmt.Printf("\n%s\n", output.Underline("Supported API Versions"))
	table := output.InitStaticTable(os.Stdout,
		output.H("API Key", tablewriter.ALIGN_CENTER),
		output.H("Name", tablewriter.ALIGN_LEFT),
		output.H("Min Version", tablewriter.ALIGN_CENTER),
		output.H("Max Version", tablewriter.ALIGN_CENTER),
	)
	for _, api := range apis {
		table.Append([]string{
			strconv.FormatInt(int64(api.Key), 10),
			output.SpaceIfEmpty(api.Name),
			strconv.FormatInt(int64(api.MinVersion), 10),
			strconv.FormatInt(int64(api.MaxVersion), 10),
		})
	}

	table.SetFooter([]string{fmt.Sprintf("Total: %d", len(apis)), " ", " ", " "})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)

	table.Render()
}

func (b *broker) printAPIPlain(apis []*kafka.API) {
	fmt.Printf("\n%s\n", output.UnderlineWithCount("Supported API Versions", len(apis)))
	for _, api := range apis {
		fmt.Printf(" %s\n", api)
	}
}
