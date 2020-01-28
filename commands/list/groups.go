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
)

type groups struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	groupFilter  *regexp.Regexp
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

	groups, err := manager.GetGroups(ctx, c.groupFilter)
	if err != nil {
		return err
	}

	if len(groups) == 0 {
		fmt.Println(internal.GetNotFoundMessage("consumer group", "group", c.groupFilter))
		return nil
	}

	sort.Strings(groups)

	switch c.format {
	case commands.PlainTextFormat:
		c.printPlainTextOutput(groups)
	case commands.TableFormat:
		c.printTableOutput(groups)
	}
	return nil
}

func (c *groups) printPlainTextOutput(groups []string) {
	for _, group := range groups {
		fmt.Printf("%s\n", group)
	}
	fmt.Printf("\nTotal: %s", humanize.Comma(int64(len(groups))))
}

func (c *groups) printTableOutput(groups []string) {
	table := output.InitStaticTable(os.Stdout, output.H("Consumer Group", tablewriter.ALIGN_LEFT))
	rows := make([][]string, 0)
	for _, group := range groups {
		rows = append(rows, []string{output.SpaceIfEmpty(group)})
	}
	table.AppendBulk(rows)
	table.SetFooter([]string{fmt.Sprintf("Total: %s", humanize.Comma(int64(len(groups))))})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}