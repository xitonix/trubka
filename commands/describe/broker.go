package describe

import (
	"fmt"
	"regexp"
	"sort"
	"strconv"

	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/tabular"
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
		NoEnvar().
		BoolVar(&cmd.includeLogs)
	c.Flag("include-api-versions", "Fetches the API versions supported by the broker.").
		NoEnvar().
		Short('a').
		BoolVar(&cmd.includeAPIVersions)
	c.Flag("topic-filter", "An optional regular expression to filter the aggregated topic logs by. Works with --include-logs only.").
		Short('t').
		NoEnvar().
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
	output.NewLines(1)
	c := internal.BoolToString(isController)
	fmt.Printf(" Controller Node: %s\n", format.Bold(c, isController && b.globalParams.EnableColor))
}

func (b *broker) printPlainTextOutput(meta *kafka.BrokerMeta) {
	b.printHeader(meta.IsController)
	fmt.Println(format.UnderlinedTitleWithCount("Consumer Groups", len(meta.ConsumerGroups)))
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
	table := tabular.NewTable(b.globalParams.EnableColor, tabular.C("Consumer Groups").Align(tabular.AlignLeft).FAlign(tabular.AlignRight))
	for _, group := range meta.ConsumerGroups {
		if len(group) > 0 {
			table.AddRow(group)
		}
	}
	table.AddFooter(fmt.Sprintf("Total: %d", len(meta.ConsumerGroups)))
	table.Render()

	if b.includeLogs && len(meta.Logs) != 0 {
		output.NewLines(2)
		b.printLogsTable(meta.Logs)
	}

	if b.includeAPIVersions && len(meta.APIs) != 0 {
		sort.Sort(kafka.APIByCode(meta.APIs))
		output.NewLines(2)
		b.printAPITable(meta.APIs)
	}
}

func (b *broker) printLogsTable(logs []*kafka.LogFile) {
	for _, logFile := range logs {
		sorted := logFile.SortByPermanentSize()
		if len(sorted) == 0 {
			msg := internal.GetNotFoundMessage("topic log", "topic", b.topicsFilter)
			fmt.Println(msg)
			return
		}
		table := tabular.NewTable(b.globalParams.EnableColor,
			tabular.C("Topic").Align(tabular.AlignLeft),
			tabular.C("Permanent Logs"),
			tabular.C("Temporary Logs"),
		)
		table.SetTitle(fmt.Sprintf("Log File Path: %s", logFile.Path))
		table.TitleAlignment(tabular.AlignLeft)
		for _, tLogs := range sorted {
			table.AddRow(
				format.SpaceIfEmpty(tLogs.Topic),
				format.SpaceIfEmpty(humanize.Bytes(tLogs.Permanent)),
				format.SpaceIfEmpty(humanize.Bytes(tLogs.Temporary)),
			)
		}
		table.Render()
	}
}

func (b *broker) printLogsPlain(logs []*kafka.LogFile) {
	for _, logFile := range logs {
		title := fmt.Sprintf("\nPath: %s", logFile.Path)
		fmt.Println(format.Underline(title))
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
	table := tabular.NewTable(b.globalParams.EnableColor,
		tabular.C("API Key"),
		tabular.C("Name").Align(tabular.AlignLeft),
		tabular.C("Min Version"),
		tabular.C("Max Version"),
	)
	table.TitleAlignment(tabular.AlignLeft)
	table.SetTitle(format.TitleWithCount("Supported API Versions", len(apis)))
	for _, api := range apis {
		table.AddRow(api.Key, format.SpaceIfEmpty(api.Name),
			strconv.FormatInt(int64(api.MinVersion), 10), api.MinVersion, api.MaxVersion)
	}

	table.AddFooter(" ", fmt.Sprintf("Total: %d", len(apis)), " ", " ")
	table.Render()
}

func (b *broker) printAPIPlain(apis []*kafka.API) {
	fmt.Println(format.UnderlinedTitleWithCount("Supported API Versions", len(apis)))
	for _, api := range apis {
		fmt.Printf(" %s\n", api)
	}
}
