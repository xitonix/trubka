package list

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type listLocalTopics struct {
	globalParams *commands.GlobalParameters
	topicsFilter *regexp.Regexp
	envFilter    *regexp.Regexp
	format       string
}

func addLocalTopicsSubCommand(parent *kingpin.CmdClause, params *commands.GlobalParameters) {
	cmd := &listLocalTopics{
		globalParams: params,
	}
	c := parent.Command("local-topics", "Lists the locally stored topics and the environments.").Action(cmd.run)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").Short('t').RegexpVar(&cmd.topicsFilter)
	c.Flag("environment-filter", "An optional case-insensitive regular expression to filter the environments by.").Short('e').RegexpVar(&cmd.envFilter)
	commands.AddFormatFlag(c, &cmd.format)
}

func (l *listLocalTopics) run(_ *kingpin.ParseContext) error {
	var err error
	l.envFilter, err = internal.IgnoreRegexCase(l.envFilter)
	if err != nil {
		return fmt.Errorf("invalid environment filter: %w", err)
	}

	offsetManager := kafka.NewLocalOffsetManager(l.globalParams.Verbosity)
	localStore, err := offsetManager.List(l.topicsFilter, l.envFilter)
	if err != nil {
		return err
	}

	if len(localStore) == 0 {
		fmt.Println("No topic offsets have been stored locally.")
	}

	switch l.format {
	case commands.PlainTextFormat:
		l.printPlainTextOutput(localStore)
	case commands.TableFormat:
		l.printTableOutput(localStore)
	}
	return nil
}

func (l *listLocalTopics) printTableOutput(store map[string][]string) {
	for env, topics := range store {
		table := commands.InitStaticTable(os.Stdout, commands.H(env, tablewriter.ALIGN_LEFT))
		table.SetColMinWidth(0, 50)
		sort.Strings(topics)
		for _, topic := range topics {
			table.Append([]string{commands.SpaceIfEmpty(topic)})
		}
		table.Render()
		fmt.Println()
	}
}

func (l *listLocalTopics) printPlainTextOutput(store map[string][]string) {
	for env, topics := range store {
		fmt.Printf("%s: %s\n", internal.Bold("Environment", l.globalParams.EnableColor), internal.Bold(env, l.globalParams.EnableColor))
		sort.Strings(topics)
		for _, topic := range topics {
			fmt.Printf("  - %s\n", topic)
		}
		fmt.Println()
	}
}
