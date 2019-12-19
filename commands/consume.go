package commands

import (
	"context"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

// AddConsumeCommand initialises the consume top level command and adds it to the application.
func AddConsumeCommand(app *kingpin.Application, global *GlobalParameters) {
	parent := app.Command("consume", "A command to consume events from Kafka.")
	kafkaParams := bindKafkaFlags(parent)
	addConsumeProtoCommand(parent, global, kafkaParams)
	addConsumePlainCommand(parent, global, kafkaParams)
}

func bindCommonConsumeFlags(command *kingpin.CmdClause,
	topic, format, environment, outputDir, logFile, from *string,
	includeTimestamp, enableAutoTopicCreation, reverse, interactive, count *bool,
	searchQuery, topicFilter **regexp.Regexp) {

	command.Arg("topic", "The Kafka topic to consume from.").StringVar(topic)

	command.Flag("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").
		Short('T').
		BoolVar(includeTimestamp)

	command.Flag("auto-topic-creation", `Enables automatic topic creation before consuming if it's allowed by the server.`).
		BoolVar(enableAutoTopicCreation)

	command.Flag("format", "The format in which the Kafka messages will be written to the output.").
		Default(internal.JsonIndent).
		EnumVar(format,
			internal.Json,
			internal.JsonIndent,
			internal.Text,
			internal.TextIndent,
			internal.Hex,
			internal.HexIndent)

	command.Flag("environment", `To store the offsets on the disk in environment specific paths. It's only required if you use Trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).
		Short('e').
		Default("local").
		StringVar(environment)

	command.Flag("search-query", "The optional regular expression to filter the message content by.").
		Short('q').
		RegexpVar(searchQuery)

	command.Flag("reverse", "If set, the messages which match the --search-query will be filtered out.").
		BoolVar(reverse)

	command.Flag("output-dir", "The `directory` to write the Kafka messages to (Default: Stdout).").
		Short('d').
		StringVar(outputDir)

	command.Flag("log-file", "The `file` to write the logs to. Set to 'none' to discard (Default: stdout).").
		Short('l').
		StringVar(logFile)

	command.Flag("interactive", "Runs the consumer in interactive mode.").
		Short('i').
		BoolVar(interactive)

	command.Flag("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").
		Short('t').
		RegexpVar(topicFilter)

	command.Flag("count", "Count the number of messages consumed from Kafka.").
		Short('c').
		BoolVar(count)

	command.Flag("from", `The offset to start consuming from. Available options are newest (default), oldest, local, time (the most recent available offset at the given time) or a string of comma separated Partition:Offset pairs ("10:150, :0")`).
		Short('f').
		Default("newest").
		StringVar(from)
}

func monitorCancellation(prn *internal.SyncPrinter, cancel context.CancelFunc) {
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
	<-signals
	prn.Info(internal.Verbose, "Stopping Trubka.")
	cancel()
}

func initialiseConsumer(kafkaParams *kafkaParameters,
	globalParams *GlobalParameters,
	environment string,
	enableAutoTopicCreation bool,
	logFile io.Writer,
	prn *internal.SyncPrinter) (*kafka.Consumer, error) {
	saramaLogWriter := ioutil.Discard
	if globalParams.Verbosity >= internal.Chatty {
		saramaLogWriter = logFile
	}

	consumer, err := kafkaParams.createConsumer(prn, environment, enableAutoTopicCreation, saramaLogWriter)
	if err != nil {
		return nil, err
	}
	return consumer, nil
}
