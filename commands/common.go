package commands

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

const (
	PlainTextFormat = "plain"
	TableFormat     = "table"
)

var ErrExitInteractiveMode = errors.New("exit")

func InitKafkaManager(globalParams *GlobalParameters, kafkaParams *KafkaParameters) (*kafka.Manager, context.Context, context.CancelFunc, error) {
	brokers := GetBrokers(kafkaParams.Brokers)
	manager, err := kafka.NewManager(brokers,
		globalParams.Verbosity,
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithTLS(kafkaParams.TLS),
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithSASL(kafkaParams.SASLMechanism,
			kafkaParams.SASLUsername,
			kafkaParams.SASLPassword))

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

func HighlightLag(input int64, colorEnabled bool) interface{} {
	humanised := humanize.Comma(input)
	if !colorEnabled {
		return humanised
	}
	if input > 0 {
		return internal.Yellow(humanised, true)
	}
	return internal.Green(humanised, true)
}

func GetNotFoundMessage(entity, filterName string, ex *regexp.Regexp) string {
	msg := fmt.Sprintf("No %s has been found.", entity)
	if ex != nil {
		msg += fmt.Sprintf(" You might need to tweak the %s filter (%s).", filterName, ex.String())
	}
	return msg
}

func Underline(in string) string {
	in = strings.TrimSpace(in)
	if len(in) == 0 {
		return ""
	}
	return in + "\n" + strings.Repeat("-", len(in))
}

func UnderlineWithCount(title string, count int) string {
	title = fmt.Sprintf("%s (%d)", title, count)
	return Underline(title)
}

func AddFormatFlag(c *kingpin.CmdClause, format *string) {
	c.Flag("format", "Sets the output format.").
		Default(TableFormat).
		Short('f').
		EnumVar(format, PlainTextFormat, TableFormat)
}

func GetBrokers(commaSeparated string) []string {
	brokers := strings.Split(commaSeparated, ",")
	for i := 0; i < len(brokers); i++ {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return brokers
}

func InitStaticTable(writer io.Writer, headers ...TableHeader) *tablewriter.Table {
	table := tablewriter.NewWriter(writer)
	headerTitles := make([]string, len(headers))
	alignments := make([]int, len(headers))
	var i int
	for _, header := range headers {
		headerTitles[i] = header.Key
		alignments[i] = header.Alignment
		i++
	}
	table.SetHeader(headerTitles)
	table.SetColumnAlignment(alignments)
	table.SetAutoWrapText(false)
	table.SetRowLine(true)
	return table
}

func SpaceIfEmpty(in string) string {
	if len(in) > 0 {
		return in
	}
	return " "
}

func FilterError(err error) error {
	if errors.Is(err, ErrExitInteractiveMode) {
		return nil
	}
	return err
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
