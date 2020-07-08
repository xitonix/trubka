package describe

import (
	"fmt"
	"regexp"
	"sort"

	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

type broker struct {
	kafkaParams        *commands.KafkaParameters
	globalParams       *commands.GlobalParameters
	topicsFilter       *regexp.Regexp
	identifier         string
	includeLogs        bool
	includeZeroLogs    bool
	includeAPIVersions bool
	format             string
	style              string
}

func addBrokerSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &broker{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("broker", "Describes a Kafka broker.").Action(cmd.run)
	c.Arg("broker", "The broker address or Id.").Required().StringVar(&cmd.identifier)
	c.Flag("include-logs", "Fetches information about the broker log for each topic.").
		Short('l').
		NoEnvar().
		BoolVar(&cmd.includeLogs)

	c.Flag("include-zero-entries", "Includes the topic log entries of size zero. Works with --include-logs only.").
		Short('z').
		NoEnvar().
		BoolVar(&cmd.includeZeroLogs)

	c.Flag("include-api-versions", "Fetches the API versions supported by the broker.").
		NoEnvar().
		Short('a').
		BoolVar(&cmd.includeAPIVersions)
	c.Flag("topic-filter", "An optional regular expression to filter the aggregated topic logs by. Works with --include-logs only.").
		Short('t').
		NoEnvar().
		RegexpVar(&cmd.topicsFilter)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
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
	case commands.JsonFormat:
		data := meta.ToJson(b.includeLogs, b.includeAPIVersions, b.includeZeroLogs)
		return output.PrintAsJson(data, b.style, b.globalParams.EnableColor)
	case commands.TableFormat:
		return b.printAsTable(meta)
	case commands.ListFormat:
		return b.printAsList(meta, false)
	case commands.PlainTextFormat:
		return b.printAsList(meta, true)
	default:
		return nil
	}
}

func (b *broker) printAsList(meta *kafka.BrokerMeta, plain bool) error {
	l := list.New(plain)
	l.AsTree()

	host := b.getHostName(meta.Details, plain)
	l.AddItem(host)

	if len(meta.ConsumerGroups) > 0 {
		b.appendConsumerGroupsToTheList(l, meta.ConsumerGroups)
	}

	if b.includeLogs && len(meta.Logs) != 0 {
		b.appendLogsToTheList(l, meta.Logs)
	}

	if b.includeAPIVersions && len(meta.APIs) != 0 {
		sort.Sort(kafka.APIByCode(meta.APIs))
		b.appendAPIsToTheList(l, meta.APIs)
	}
	l.Render()
	return nil
}

func (b *broker) printAsTable(meta *kafka.BrokerMeta) error {
	host := b.getHostName(meta.Details, false)
	if len(meta.ConsumerGroups) > 0 {
		host = fmt.Sprintf("Consumer Groups (%s)", host)
	}
	table := tabular.NewTable(b.globalParams.EnableColor, tabular.C(host).Align(tabular.AlignLeft).FAlign(tabular.AlignRight))
	if len(meta.ConsumerGroups) > 0 {
		for _, group := range meta.ConsumerGroups {
			if len(group) > 0 {
				table.AddRow(group)
			}
		}
		table.AddFooter(fmt.Sprintf("Total: %d", len(meta.ConsumerGroups)))
	}
	table.Render()

	if b.includeLogs && len(meta.Logs) != 0 {
		output.NewLines(1)
		if err := b.printLogsTable(meta.Logs); err != nil {
			return err
		}
	}

	if b.includeAPIVersions && len(meta.APIs) != 0 {
		sort.Sort(kafka.APIByCode(meta.APIs))
		output.NewLines(1)
		b.printAPITable(meta.APIs)
	}
	return nil
}

func (b *broker) printLogsTable(logs []*kafka.LogFile) error {
	for _, logFile := range logs {
		sorted := logFile.SortByPermanentSize()
		if len(sorted) == 0 {
			return internal.NotFoundError("topic log", "topic", b.topicsFilter)
		}
		table := tabular.NewTable(b.globalParams.EnableColor,
			tabular.C("Topic").Align(tabular.AlignLeft).FAlign(tabular.AlignRight),
			tabular.C("Permanent Logs").FAlign(tabular.AlignCenter),
			tabular.C("Temporary Logs").FAlign(tabular.AlignCenter),
		)
		table.SetTitle(fmt.Sprintf("Log File Path: %s", logFile.Path))
		table.TitleAlignment(tabular.AlignLeft)
		var totalPerm, totalTemp uint64
		for _, tLogs := range sorted {
			if !b.includeZeroLogs && tLogs.Permanent == 0 && tLogs.Temporary == 0 {
				continue
			}
			totalPerm += tLogs.Permanent
			totalTemp += tLogs.Temporary
			table.AddRow(
				format.SpaceIfEmpty(tLogs.Topic),
				format.SpaceIfEmpty(humanize.Bytes(tLogs.Permanent)),
				format.SpaceIfEmpty(humanize.Bytes(tLogs.Temporary)),
			)
		}

		table.AddFooter("Total", format.SpaceIfEmpty(humanize.Bytes(totalPerm)), format.SpaceIfEmpty(humanize.Bytes(totalTemp)))
		table.Render()
	}
	return nil
}

func (b *broker) appendConsumerGroupsToTheList(l list.List, groups []string) {
	l.Indent()
	l.AddItem("Consumer Groups")
	l.Indent()
	for _, group := range groups {
		l.AddItem(group)
	}
	l.UnIndent()
}

func (b *broker) appendLogsToTheList(l list.List, logs []*kafka.LogFile) {
	l.AddItem("Logs")
	l.Indent()
	for _, logFile := range logs {
		l.AddItem(logFile.Path)
		sorted := logFile.SortByPermanentSize()
		l.Indent()
		for _, tLogs := range sorted {
			if !b.includeZeroLogs && tLogs.Permanent == 0 && tLogs.Temporary == 0 {
				continue
			}
			l.AddItem(tLogs.Topic)
			l.Indent()
			if b.includeZeroLogs || tLogs.Permanent > 0 {
				l.AddItem("Permanent: " + humanize.Bytes(tLogs.Permanent))
			}
			if b.includeZeroLogs || tLogs.Temporary > 0 {
				l.AddItem("Temporary: " + humanize.Bytes(tLogs.Temporary))
			}
			l.UnIndent()
		}
		l.UnIndent()
	}
	l.UnIndent()
}

func (b *broker) printAPITable(apis []*kafka.API) {
	table := tabular.NewTable(b.globalParams.EnableColor,
		tabular.C("API Key"),
		tabular.C("Name").Align(tabular.AlignLeft),
		tabular.C("Min Version"),
		tabular.C("Max Version"),
	)
	table.TitleAlignment(tabular.AlignLeft)
	table.SetTitle(format.WithCount("Supported API Versions", len(apis)))
	for _, api := range apis {
		table.AddRow(api.Key, format.SpaceIfEmpty(api.Name), api.MinVersion, api.MaxVersion)
	}

	table.AddFooter(" ", fmt.Sprintf("Total: %d", len(apis)), " ", " ")
	table.Render()
}

func (b *broker) appendAPIsToTheList(l list.List, apis []*kafka.API) {
	l.AddItem("Supported API Versions")
	l.Indent()
	for _, api := range apis {
		l.AddItem(api)
	}
	l.UnIndent()
}

func (b *broker) getHostName(broker *kafka.Broker, plain bool) string {
	host := broker.String()
	if broker.IsController {
		host = fmt.Sprintf("%s %v", host, format.GreenLabel(controlNodeFlag, b.globalParams.EnableColor && !plain))
	}
	return host
}
