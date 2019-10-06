package commands

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"syscall"
	"time"

	"github.com/dustin/go-humanize"
	"github.com/gookit/color"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

const (
	plainTextFormat = "plain"
	tableFormat     = "table"
)

var (
	yellow = color.Warn.Render
	green  = color.Info.Render
	bold   = color.Bold.Render
)

func initKafkaManager(globalParams *GlobalParameters, kafkaParams *kafkaParameters) (*kafka.Manager, context.Context, context.CancelFunc, error) {
	brokers := getBrokers(kafkaParams.brokers)
	manager, err := kafka.NewManager(brokers,
		globalParams.Verbosity,
		kafka.WithClusterVersion(kafkaParams.version),
		kafka.WithTLS(kafkaParams.tls),
		kafka.WithClusterVersion(kafkaParams.version),
		kafka.WithSASL(kafkaParams.saslMechanism,
			kafkaParams.saslUsername,
			kafkaParams.saslPassword))

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

func highlightLag(input int64) string {
	if input > 0 {
		return yellow(humanize.Comma(input))
	}
	return green(humanize.Comma(input))
}

func getNotFoundMessage(entity, filterName string, ex *regexp.Regexp) string {
	msg := fmt.Sprintf("No %s has been found.", entity)
	if ex != nil {
		msg += fmt.Sprintf(" You might need to tweak the %s filter (%s).", filterName, ex.String())
	}
	return msg
}

func addFormatFlag(c *kingpin.CmdClause, format *string) {
	c.Flag("format", "Sets the output format.").
		Default(tableFormat).
		Short('f').
		EnumVar(format, plainTextFormat, tableFormat)
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
			return nil, false, errors.Wrapf(err, "Failed to create: %s", logFile)
		}
		return lf, true, nil
	}
}

func getTopics(topicMap map[string]string, cp *kafka.Checkpoint) map[string]*kafka.Checkpoint {
	topics := make(map[string]*kafka.Checkpoint)
	for topic := range topicMap {
		topics[topic] = cp
	}
	return topics
}

func closeFile(file *os.File) {
	err := file.Sync()
	if err != nil {
		color.Error.Printf("Failed to sync the file: %s\n", err)
	}
	if err := file.Close(); err != nil {
		color.Error.Printf("Failed to close the file: %s\n", err)
	}
}

func getCheckpoint(rewind bool, offsetCheckpoint int64, timeCheckpoint time.Time) *kafka.Checkpoint {
	cp := kafka.NewCheckpoint(rewind)
	switch {
	case offsetCheckpoint != -1:
		cp.SetOffset(offsetCheckpoint)
	case !timeCheckpoint.IsZero():
		cp.SetTimeOffset(timeCheckpoint)
	}
	return cp
}

func getOutputWriters(outputDir string, topics map[string]*kafka.Checkpoint) (map[string]io.Writer, bool, error) {
	result := make(map[string]io.Writer)

	if internal.IsEmpty(outputDir) {
		for topic := range topics {
			result[topic] = os.Stdout
		}
		return result, false, nil
	}

	err := os.MkdirAll(outputDir, 0755)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to create the output directory")
	}

	for topic := range topics {
		file := filepath.Join(outputDir, topic)
		lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
		}
		result[topic] = lf
	}

	return result, true, nil
}
