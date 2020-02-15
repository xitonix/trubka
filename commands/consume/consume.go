package consume

import (
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

// AddCommands initialises the consume top level command and adds it to the application.
func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("consume", "A command to consume events from Kafka.")
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
			internal.HexIndent,
			internal.Base64)

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

func initialiseConsumer(kafkaParams *commands.KafkaParameters,
	globalParams *commands.GlobalParameters,
	environment string,
	enableAutoTopicCreation bool,
	logFile io.Writer,
	prn *internal.SyncPrinter) (*kafka.Consumer, error) {
	saramaLogWriter := ioutil.Discard
	if globalParams.Verbosity >= internal.Chatty {
		saramaLogWriter = logFile
	}

	brokers := commands.GetBrokers(kafkaParams.Brokers)
	consumer, err := kafka.NewConsumer(
		brokers, prn,
		environment,
		enableAutoTopicCreation,
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithTLS(kafkaParams.TLS),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(kafkaParams.SASLMechanism,
			kafkaParams.SASLUsername,
			kafkaParams.SASLPassword))

	if err != nil {
		return nil, err
	}
	return consumer, nil
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

func filterError(err error) error {
	if errors.Is(err, errExitInteractiveMode) {
		return nil
	}
	return err
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
