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
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/kafka"
)

// AddCommands initialises the consume top level command and adds it to the application.
func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("consume", "A command to consume events from Kafka.")
	addConsumeProtoCommand(parent, global, kafkaParams)
	addConsumePlainCommand(parent, global, kafkaParams)
}

func bindCommonConsumeFlags(command *kingpin.CmdClause,
	topic, environment, outputDir, logFile *string,
	from, to *[]string,
	exclusive *bool,
	idleTimeout *time.Duration,
	inclusions *internal.MessageMetadata,
	enableAutoTopicCreation, reverse, interactive, interactiveWithCustomOffset, count *bool,
	searchQuery, topicFilter **regexp.Regexp, highlightStyle *string) {

	command.Arg("topic", "The Kafka topic to consume from.").StringVar(topic)

	command.Flag("include-partition", "Prints the partition to which the message belongs.").
		Short('P').
		BoolVar(&inclusions.Partition)

	command.Flag("include-partition-key", "Prints the partition key of each message.").
		Short('K').
		BoolVar(&inclusions.Key)

	command.Flag("include-topic-name", "Prints the topic name from which the message was consumed.").
		Short('T').
		BoolVar(&inclusions.Topic)

	command.Flag("include-offset", "Prints the partition offset.").
		Short('O').
		BoolVar(&inclusions.Offset)

	command.Flag("include-timestamp", "Prints the message timestamp if it has been provided by Kafka.").
		Short('S').
		BoolVar(&inclusions.Timestamp)

	command.Flag("auto-topic-creation", `Enables automatic topic creation before consuming if it's allowed by the server.`).
		BoolVar(enableAutoTopicCreation)

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

	now := time.Now()

	ts := internal.FormatTime(now.Add(-2 * time.Hour))
	help := fmt.Sprintf("The offset to start consuming from. "+
		"Available options are newest, oldest, local, timestamp, offset, Partition#Offset or Partition#Timestamp (eg. \"10#300\", \"7#%s\")", ts)
	command.Flag("from", help).
		Default("newest").
		HintOptions("newest", "oldest", ts, "8000").
		StringsVar(from)

	ts = internal.FormatTime(now.Add(-1 * time.Hour))
	help = fmt.Sprintf("The offset where trubka must stop consuming. "+
		"Available options are timestamp, offset, Partition#Offset or Partition#Timestamp (eg. \"10#800\", \"7#%s\")", ts)
	command.Flag("to", help).
		HintOptions(ts, "9000").
		StringsVar(to)

	command.Flag("exclusive", `Only explicitly defined partitions (Partition#Offset or Partition#Timestamp) will be consumed. The rest will be excluded.`).
		Short('E').
		BoolVar(exclusive)

	min := 2 * time.Second
	help = fmt.Sprintf(`The amount of time the consumer will wait for a message to arrive before stop consuming from a partition (Minimum: %s)`, min)
	command.Flag("idle-timeout", help).
		PreAction(func(parseContext *kingpin.ParseContext) error {
			if *idleTimeout < min {
				*idleTimeout = min
			}
			return nil
		}).
		DurationVar(idleTimeout)

	command.Flag("style", fmt.Sprintf("The highlighting style of the Json output. Applicable to --encode-to=%s only. Set to 'none' to disable.", internal.JsonIndentEncoding)).
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
	exclusive bool,
	idleTimeout time.Duration,
	logFile io.Writer,
	printer internal.Printer) (*kafka.Consumer, error) {
	saramaLogWriter := ioutil.Discard
	if globalParams.Verbosity >= internal.Chatty {
		saramaLogWriter = logFile
	}

	brokers := commands.GetBrokers(kafkaParams.Brokers)

	store, err := kafka.NewLocalOffsetStore(printer, environment)
	if err != nil {
		return nil, err
	}

	wrapper, err := kafka.NewConsumerWrapper(brokers, kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithTLS(kafkaParams.TLS),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(kafkaParams.SASLMechanism,
			kafkaParams.SASLUsername,
			kafkaParams.SASLPassword,
			kafkaParams.SASLHandshakeVersion))

	consumer, err := kafka.NewConsumer(
		store,
		wrapper,
		printer,
		enableAutoTopicCreation,
		exclusive,
		idleTimeout,
	)

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
		fmt.Println(format.Red(msg, highlight))
	}
	if err := file.Close(); err != nil {
		msg := fmt.Sprintf("Failed to close the file: %s", err)
		fmt.Println(format.Red(msg, highlight))
	}
}
