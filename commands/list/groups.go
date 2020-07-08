package list

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

type groups struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	groupFilter  *regexp.Regexp
	includeState bool
	format       string
	style        string
}

func addGroupsSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &groups{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("groups", "Loads the consumer groups from the server.").Action(cmd.run)
	c.Flag("group-filter", "An optional regular expression to filter the groups by.").
		Short('g').
		RegexpVar(&cmd.groupFilter)

	c.Flag("include-states", "Include consumer groups' state information.").
		Short('s').
		BoolVar(&cmd.includeState)

	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
}

func (c *groups) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	groups, err := manager.GetGroups(ctx, c.groupFilter, c.includeState)
	if err != nil {
		return err
	}

	if len(groups) == 0 {
		return internal.NotFoundError("consumer group", "group", c.groupFilter)
	}

	sort.Sort(kafka.ConsumerGroupDetailsByName(groups))

	switch c.format {
	case commands.JsonFormat:
		data := make([]interface{}, len(groups))
		for i, g := range groups {
			data[i] = g.ToJson(false)
		}
		return output.PrintAsJson(data, c.style, c.globalParams.EnableColor)
	case commands.TableFormat:
		return c.printAsTable(groups)
	case commands.TreeFormat:
		return c.printAsList(groups, false)
	case commands.PlainTextFormat:
		return c.printAsList(groups, true)
	default:
		return nil
	}
}

func (c *groups) printAsList(groups []*kafka.ConsumerGroupDetails, plain bool) error {
	l := list.New(plain)
	for _, group := range groups {
		if c.includeState {
			l.AddItem(group.Name)
			l.Indent()
			l.AddItemF("State: %s", format.GroupStateLabel(group.State, c.globalParams.EnableColor && !plain))
			l.AddItemF("Protocol: %s/%s", group.Protocol, group.ProtocolType)
			l.AddItemF("Coordinator: %s", group.Coordinator.String())
			l.UnIndent()
		} else {
			l.AddItem(group.Name)
		}
	}
	l.Render()
	return nil
}

func (c *groups) printAsTable(groups []*kafka.ConsumerGroupDetails) error {
	var table *tabular.Table
	if c.includeState {
		table = tabular.NewTable(c.globalParams.EnableColor,
			tabular.C("Name").Align(tabular.AlignLeft),
			tabular.C("State"),
			tabular.C("Protocol"),
			tabular.C("Protocol Type"),
			tabular.C("Coordinator"),
		)
	} else {
		table = tabular.NewTable(c.globalParams.EnableColor, tabular.C("Consumer Group").Align(tabular.AlignLeft))
	}

	for _, group := range groups {
		if c.includeState {
			table.AddRow(group.Name,
				format.GroupStateLabel(group.State, c.globalParams.EnableColor),
				group.Protocol,
				group.ProtocolType,
				group.Coordinator.Host)
		} else {
			table.AddRow(group.Name)
		}
	}
	if c.includeState {
		table.AddFooter(fmt.Sprintf("Total: %s", humanize.Comma(int64(len(groups)))), " ", " ", " ", " ")
	} else {
		table.AddFooter(fmt.Sprintf("Total: %s", humanize.Comma(int64(len(groups)))))
	}
	table.Render()
	return nil
}
