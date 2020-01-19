package commands

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"regexp"
	"time"

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
	includeTimestamp, includeKey, includeTopicName, enableAutoTopicCreation, reverse, interactive, interactiveWithCustomOffset, count *bool,
	searchQuery, topicFilter **regexp.Regexp, highlightStyle *string) {

	command.Arg("topic", "The Kafka topic to consume from.").StringVar(topic)

	command.Flag("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").
		Short('S').
		BoolVar(includeTimestamp)

	command.Flag("include-partition-key", "Prints the partition key before the content.").
		Short('K').
		BoolVar(includeKey)

	command.Flag("include-topic-name", "Prints the topic name before the content.").
		Short('T').
		BoolVar(includeTopicName)

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

	command.Flag("interactive", "Runs the consumer in interactive mode. Use --interactive-with-offset to set the starting offset for each topic.").
		Short('i').
		BoolVar(interactive)

	command.Flag("interactive-with-offset", "Runs the consumer in interactive mode. In this mode, you will be able to define the starting offset for each topic.").
		Short('I').
		BoolVar(interactiveWithCustomOffset)

	command.Flag("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").
		Short('t').
		RegexpVar(topicFilter)

	command.Flag("count", "Count the number of messages consumed from Kafka.").
		Short('c').
		BoolVar(count)

	pastHour := time.Now().UTC().Add(-1 * time.Hour).Format("02-01-2006T15:04:05.999999999")
	command.Flag("from", `The offset to start consuming from. Available options are newest (default), oldest, local, time (the most recent available offset at the given time) or a string of comma separated Partition:Offset pairs ("10:150, :0")`).
		Short('f').
		Default("newest").
		HintOptions("newest", "oldest", pastHour, "0:10,1:20,:0").
		StringVar(from)

	command.Flag("style", fmt.Sprintf("The highlighting style of the Json output. Applicable to --format=%s only. Set to 'none' to disable.", internal.JsonIndent)).
		Default(internal.DefaultHighlightStyle).
		EnumVar(highlightStyle,
			internal.HighlightStyles...)
}

func monitorCancellation(prn *internal.SyncPrinter, cancel context.CancelFunc) {
	internal.WaitForCancellationSignal()
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
