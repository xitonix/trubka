package commands

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

const (
	// PlainTextFormat plain text format.
	PlainTextFormat = "plain"
	// TableFormat tabular format.
	TableFormat = "table"
	// ListFormat list format.
	ListFormat = "list"
	// JsonFormat json format.
	JsonFormat = "json"
)

// InitKafkaManager initialises the Kafka manager.
func InitKafkaManager(globalParams *GlobalParameters, kafkaParams *KafkaParameters) (*kafka.Manager, context.Context, context.CancelFunc, error) {
	brokers := GetBrokers(kafkaParams.Brokers)
	manager, err := kafka.NewManager(brokers,
		globalParams.Verbosity,
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithTLS(kafkaParams.TLS),
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithSASL(kafkaParams.SASLMechanism,
			kafkaParams.SASLUsername,
			kafkaParams.SASLPassword,
			kafkaParams.SASLHandshakeVersion))

	if err != nil {
		return nil, nil, nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		cancel()
	}()

	return manager, ctx, cancel, nil
}

// GetBrokers returns the list of the brokers.
func GetBrokers(commaSeparated string) []string {
	brokers := strings.Split(commaSeparated, ",")
	for i := 0; i < len(brokers); i++ {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return brokers
}

// AddFormatFlag adds the format flag to the specified command.
func AddFormatFlag(c *kingpin.CmdClause, format *string, style *string) {
	c.Flag("format", "Sets the output format.").
		Default(TableFormat).
		NoEnvar().
		Short('f').
		EnumVar(format, PlainTextFormat, TableFormat, ListFormat, JsonFormat)

	c.Flag("style", fmt.Sprintf("The highlighting style of the Json output. Applicable to --format=%s only. Disabled (none) by default.", JsonFormat)).
		Default("none").
		EnumVar(style, internal.HighlightStyles...)
}

// PrintConfigTable prints the configurations in tabular format.
func PrintConfigTable(entries []*kafka.ConfigEntry) {
	sort.Sort(kafka.ConfigEntriesByName(entries))
	table := tabular.NewTable(true,
		tabular.C("Name").Align(tabular.AlignLeft).MaxWidth(100),
		tabular.C("Value").Align(tabular.AlignLeft).FAlign(tabular.AlignRight).MaxWidth(100),
	)
	table.SetTitle(format.WithCount("Configurations", len(entries)))
	for _, config := range entries {
		parts := strings.Split(config.Value, ",")
		table.AddRow(config.Name, strings.Join(parts, "\n"))
	}
	table.AddFooter("", fmt.Sprintf("Total: %d", len(entries)))
	table.Render()
}

// PrintConfigList prints the configurations as a list.
func PrintConfigList(l list.List, entries []*kafka.ConfigEntry, plain bool) {
	sort.Sort(kafka.ConfigEntriesByName(entries))
	l.AddItem("Configurations")
	l.Indent()
	for _, config := range entries {
		if plain {
			l.AddItemF("%s: %v", config.Name, config.Value)
			continue
		}
		parts := strings.Split(config.Value, ",")
		if len(parts) == 1 {
			l.AddItemF("%s: %v", config.Name, config.Value)
			continue
		}
		l.AddItem(config.Name)
		l.Indent()
		for _, val := range parts {
			if !internal.IsEmpty(val) {
				l.AddItem(val)
			}
		}
		l.UnIndent()
	}
}

// AskForConfirmation asks the user for confirmation. The user must type in "yes/y", "no/n" or "exit/quit/q"
// and then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
func AskForConfirmation(s string) bool {
	scanner := bufio.NewScanner(os.Stdin)
	msg := fmt.Sprintf("%s [y/n]?: ", s)
	for fmt.Print(msg); scanner.Scan(); fmt.Print(msg) {
		r := strings.ToLower(strings.TrimSpace(scanner.Text()))
		switch r {
		case "y", "yes":
			return true
		case "n", "no", "q", "quit", "exit":
			return false
		}
	}
	return false
}
