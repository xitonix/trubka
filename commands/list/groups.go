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

	commands.AddFormatFlag(c, &cmd.format)
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
		fmt.Println(internal.GetNotFoundMessage("consumer group", "group", c.groupFilter))
		return nil
	}

	sort.Sort(kafka.ConsumerGroupDetailsByName(groups))

	switch c.format {
	case commands.PlainTextFormat:
		c.printPlainTextOutput(groups)
	case commands.TableFormat:
		c.printTableOutput(groups)
	}
	return nil
}

func (c *groups) printPlainTextOutput(groups []*kafka.ConsumerGroupDetails) {
	if c.includeState {
		for _, group := range groups {
			b := list.NewBullet()
			b.AsTree()
			b.AddItem(group.Name)
			b.Intend()
			b.AddItem(fmt.Sprintf("        State: %s", format.GroupStateLabel(group.State, c.globalParams.EnableColor)))
			b.AddItem(fmt.Sprintf("     Protocol: %s", group.Protocol))
			b.AddItem(fmt.Sprintf("Protocol Type: %s", group.ProtocolType))
			b.AddItem(fmt.Sprintf("  Coordinator: %s", group.Coordinator.Host))
			b.UnIntend()
			b.Render()
			output.NewLines(1)
		}
	} else {
		b := list.NewBullet()
		for _, group := range groups {
			b.AddItem(group.Name)
		}
		b.Render()
		output.NewLines(1)
	}
	fmt.Printf("%s\n %s", format.Underline("Total"), humanize.Comma(int64(len(groups))))
}

func (c *groups) printTableOutput(groups []*kafka.ConsumerGroupDetails) {
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
}
