package list

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
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
	for _, group := range groups {
		if c.includeState {
			fmt.Printf("%s\n\n", group)
		} else {
			fmt.Printf("%s\n", group.Name)
		}
	}
	fmt.Printf("\nTotal: %s", humanize.Comma(int64(len(groups))))
}

func (c *groups) printTableOutput(groups []*kafka.ConsumerGroupDetails) {
	var table *tablewriter.Table
	if c.includeState {
		table = output.InitStaticTable(os.Stdout,
			output.H("Name", tablewriter.ALIGN_LEFT),
			output.H("State", tablewriter.ALIGN_CENTER),
			output.H("Protocol", tablewriter.ALIGN_CENTER),
			output.H("Protocol Type", tablewriter.ALIGN_CENTER),
			output.H("Coordinator", tablewriter.ALIGN_CENTER),
		)
	} else {
		table = output.InitStaticTable(os.Stdout, output.H("Consumer Group", tablewriter.ALIGN_LEFT))
	}

	rows := make([][]string, 0)
	for _, group := range groups {
		if c.includeState {
			rows = append(rows, []string{
				group.Name,
				internal.HighlightGroupState(group.State, c.globalParams.EnableColor),
				group.Protocol,
				group.ProtocolType,
				group.Coordinator.Host})
		} else {
			rows = append(rows, []string{group.Name})
		}
	}
	table.AppendBulk(rows)
	if c.includeState {
		table.SetFooter([]string{fmt.Sprintf("Total: %s", humanize.Comma(int64(len(groups)))), " ", " ", " ", " "})
	} else {
		table.SetFooter([]string{fmt.Sprintf("Total: %s", humanize.Comma(int64(len(groups))))})
	}
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
