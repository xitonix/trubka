package commands

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
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

func InitKafkaManager(globalParams *GlobalParameters, kafkaParams *KafkaParameters) (*kafka.Manager, context.Context, context.CancelFunc, error) {
	brokers := getBrokers(kafkaParams.Brokers)
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

func AddFormatFlag(c *kingpin.CmdClause, format *string) {
	c.Flag("format", "Sets the output format.").
		Default(TableFormat).
		Short('f').
		EnumVar(format, PlainTextFormat, TableFormat)
}

func getBrokers(commaSeparated string) []string {
	brokers := strings.Split(commaSeparated, ",")
	for i := 0; i < len(brokers); i++ {
		brokers[i] = strings.TrimSpace(brokers[i])
	}
	return brokers
}

func getLogWriter(logFile string) (io.Writer, bool, error) {
	switch strings.TrimSpace(strings.ToLower(logFile)) {
	case "none":
		return ioutil.Discard, false, nil
	case "":
		return os.Stdout, false, nil
	default:
		lf, err := os.OpenFile(logFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create %s: %w", logFile, err)
		}
		return lf, true, nil
	}
}

func getTopics(topicMap map[string]string, checkpoints *kafka.PartitionCheckpoints) map[string]*kafka.PartitionCheckpoints {
	topics := make(map[string]*kafka.PartitionCheckpoints)
	for topic := range topicMap {
		topics[topic] = checkpoints
	}
	return topics
}

func closeFile(file *os.File, highlight bool) {
	err := file.Sync()
	if err != nil {
		msg := fmt.Sprintf("Failed to sync the file: %s", err)
		fmt.Println(internal.Red(msg, highlight))
	}
	if err := file.Close(); err != nil {
		msg := fmt.Sprintf("Failed to close the file: %s", err)
		fmt.Println(internal.Red(msg, highlight))
	}
}

func InitStaticTable(writer io.Writer, headers map[string]int) *tablewriter.Table {
	table := tablewriter.NewWriter(writer)
	headerTitles := make([]string, len(headers))
	alignments := make([]int, len(headers))
	var i int
	for h, a := range headers {
		headerTitles[i] = h
		alignments[i] = a
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

func getOutputWriters(outputDir string, topics map[string]*kafka.PartitionCheckpoints) (map[string]io.Writer, bool, error) {
	result := make(map[string]io.Writer)

	if internal.IsEmpty(outputDir) {
		for topic := range topics {
			result[topic] = os.Stdout
		}
		return result, false, nil
	}

	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return nil, false, fmt.Errorf("failed to create the output directory: %w", err)
	}

	for topic := range topics {
		file := filepath.Join(outputDir, topic)
		lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, fmt.Errorf("failed to create %s: %w", file, err)
		}
		result[topic] = lf
	}

	return result, true, nil
}

func filterError(err error) error {
	if errors.Is(err, errExitInteractiveMode) {
		return nil
	}
	return err
}
