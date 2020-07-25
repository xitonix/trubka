package list

import (
	"fmt"
	"os"
	"regexp"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

type listLocalTopics struct {
	globalParams *commands.GlobalParameters
	topicsFilter *regexp.Regexp
	envFilter    *regexp.Regexp
	format       string
	style        string
}

func addLocalTopicsSubCommand(parent *kingpin.CmdClause, params *commands.GlobalParameters) {
	cmd := &listLocalTopics{
		globalParams: params,
	}
	c := parent.Command("local-topics", "Lists the locally stored topics and the environments.").Action(cmd.run)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").Short('t').RegexpVar(&cmd.topicsFilter)
	c.Flag("environment-filter", "An optional case-insensitive regular expression to filter the environments by.").Short('e').RegexpVar(&cmd.envFilter)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
}

func (l *listLocalTopics) run(_ *kingpin.ParseContext) error {
	var err error
	l.envFilter, err = internal.IgnoreRegexCase(l.envFilter)
	if err != nil {
		return fmt.Errorf("invalid environment filter: %w", err)
	}

	offsetManager := kafka.NewLocalOffsetManager(internal.NewPrinter(l.globalParams.Verbosity, os.Stdout))
	localStore, err := offsetManager.List(l.topicsFilter, l.envFilter)
	if err != nil {
		return err
	}

	if len(localStore) == 0 {
		fmt.Println("No topic offsets have been stored locally.")
	}

	switch l.format {
	case commands.JsonFormat:
		return output.PrintAsJson(localStore, l.style, l.globalParams.EnableColor)
	case commands.TableFormat:
		return l.printAsTable(localStore)
	case commands.TreeFormat:
		return l.printAsList(localStore, false)
	case commands.PlainTextFormat:
		return l.printAsList(localStore, true)
	}
	return nil
}

func (l *listLocalTopics) printAsTable(store map[string][]string) error {
	for env, topics := range store {
		table := tabular.NewTable(l.globalParams.EnableColor, tabular.C(format.WithCount(env, len(topics))).Align(tabular.AlignLeft).MinWidth(60))
		sort.Strings(topics)
		for _, topic := range topics {
			table.AddRow(format.SpaceIfEmpty(topic))
		}
		table.AddFooter(fmt.Sprintf("Total: %d", len(topics)))
		table.Render()
		output.NewLines(1)
	}
	return nil
}

func (l *listLocalTopics) printAsList(store map[string][]string, plain bool) error {
	ls := list.New(plain)
	for env, topics := range store {
		ls.AddItem(env)
		ls.Indent()
		sort.Strings(topics)
		for _, topic := range topics {
			ls.AddItem(topic)
		}
		ls.UnIndent()
	}
	ls.Render()
	return nil
}
