package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"syscall"

	"github.com/gookit/color"
	"gopkg.in/alecthomas/kingpin.v2"

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
		return yellow(input)
	}
	return green(input)
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
