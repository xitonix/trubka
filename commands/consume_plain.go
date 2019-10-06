package commands

import (
	"bytes"
	"context"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"github.com/gookit/color"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

type consumePlain struct {
	globalParams *GlobalParameters
	kafkaParams  *kafkaParameters

	topic                   string
	format                  string
	outputDir               string
	environment             string
	logFile                 string
	searchQuery             *regexp.Regexp
	rewind                  bool
	reverse                 bool
	includeTimestamp        bool
	enableAutoTopicCreation bool
	timeCheckpoint          time.Time
	offsetCheckpoint        int64
}

func addConsumePlainCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &consumePlain{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("plain", "Starts consuming plain text events from the given Kafka topic.").Action(cmd.run)
	cmd.bindCommandFlags(c)
}

func (c *consumePlain) bindCommandFlags(command *kingpin.CmdClause) {
	command.Arg("topic", "The Kafka topic to consume from.").Required().StringVar(&c.topic)
	command.Flag("rewind", "Starts consuming from the beginning of the stream.").Short('w').BoolVar(&c.rewind)
	var timeCheckpoint string
	command.Flag("from", "Starts consuming from the most recent available offset at the given time. This will override --rewind.").
		Short('f').
		Action(func(context *kingpin.ParseContext) error {
			t, err := parseTime(timeCheckpoint)
			if err != nil {
				return err
			}
			c.timeCheckpoint = t
			return nil
		}).
		StringVar(&timeCheckpoint)
	command.Flag("from-offset", `Starts consuming from the specified offset (if applicable). This will override --rewind and --from.
							If the most recent offset value of a partition is less than the specified value, this flag will be ignored.`).
		Default("-1").
		Short('o').
		Int64Var(&c.offsetCheckpoint)
	command.Flag("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").
		Short('T').
		BoolVar(&c.includeTimestamp)
	command.Flag("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
							Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`).
		BoolVar(&c.enableAutoTopicCreation)

	command.Flag("format", "The format in which the Kafka messages will be written to the output.").
		Default(protobuf.JsonIndent).
		EnumVar(&c.format,
			protobuf.Json,
			protobuf.JsonIndent,
			protobuf.Text,
			protobuf.TextIndent,
			protobuf.Hex,
			protobuf.HexIndent)
	command.Flag("output-dir", "The `directory` to write the Kafka messages to (Default: Stdout).").
		Short('d').
		StringVar(&c.outputDir)
	command.Flag("environment", `To store the offsets on the disk in environment specific paths. It's only required
									if you use Trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).
		Short('e').
		Default("local").
		StringVar(&c.environment)

	command.Flag("reverse", "If set, the messages which match the --search-query will be filtered out.").
		BoolVar(&c.reverse)

	command.Flag("search-query", "The optional regular expression to filter the message content by.").
		Short('q').
		RegexpVar(&c.searchQuery)
	command.Flag("log-file", "The `file` to write the logs to. Set to 'none' to discard (Default: stdout).").
		Short('l').
		StringVar(&c.logFile)
}

func (c *consumePlain) run(_ *kingpin.ParseContext) error {
	logFile, writeLogToFile, err := getLogWriter(c.logFile)
	if err != nil {
		return err
	}

	prn := internal.NewPrinter(c.globalParams.Verbosity, logFile)

	saramaLogWriter := ioutil.Discard
	if c.globalParams.Verbosity >= internal.Chatty {
		saramaLogWriter = logFile
	}

	brokers := getBrokers(c.kafkaParams.brokers)
	consumer, err := kafka.NewConsumer(
		brokers, prn,
		c.environment,
		c.enableAutoTopicCreation,
		kafka.WithClusterVersion(c.kafkaParams.version),
		kafka.WithTLS(c.kafkaParams.tls),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(c.kafkaParams.saslMechanism,
			c.kafkaParams.saslUsername,
			c.kafkaParams.saslPassword))

	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		prn.Info(internal.Verbose, "Stopping Trubka.")
		cancel()
	}()

	cp := getCheckpoint(c.rewind, c.offsetCheckpoint, c.timeCheckpoint)
	topics := map[string]*kafka.Checkpoint{
		c.topic: cp,
	}

	writers, writeEventsToFile, err := getOutputWriters(c.outputDir, topics)
	if err != nil {
		return err
	}

	prn.Start(writers)

	wg := sync.WaitGroup{}

	wg.Add(1)
	consumerCtx, stopConsumer := context.WithCancel(context.Background())
	defer stopConsumer()
	go func() {
		defer wg.Done()
		marshaller := internal.NewPlainTextMarshaller(c.format, c.includeTimestamp)
		var searchColor color.Style
		if !writeEventsToFile {
			searchColor = color.Warn.Style
		}
		var cancelled bool
		for {
			select {
			case <-ctx.Done():
				if !cancelled {
					stopConsumer()
					cancelled = true
				}
			case event, more := <-consumer.Events():
				if !more {
					return
				}
				if cancelled {
					// We keep consuming and let the Events channel to drain
					// Otherwise the consumer will deadlock
					continue
				}
				output, err := c.process(event, searchColor, marshaller)
				if err == nil {
					prn.WriteEvent(event.Topic, output)
					consumer.StoreOffset(event)
					continue
				}
				prn.Errorf(internal.Forced,
					"Failed to process the message at offset %d of partition %d, topic %s: %s",
					event.Offset,
					event.Partition,
					event.Topic,
					err)
			}
		}
	}()
	err = consumer.Start(consumerCtx, topics)
	if err != nil {
		prn.Errorf(internal.Forced, "Failed to start the consumer: %s", err)
	}

	// We still need to explicitly close the underlying Kafka client, in case `consumer.Start` has not been called.
	// It is safe to close the consumer twice.
	consumer.Close()
	wg.Wait()

	if err != nil {
		return err
	}

	// Do not write to Printer after this point
	if writeLogToFile {
		closeFile(logFile.(*os.File))
	}

	if writeEventsToFile {
		for _, w := range writers {
			closeFile(w.(*os.File))
		}
	}
	prn.Close()

	return nil
}

func (c *consumePlain) process(event *kafka.Event, highlightColor color.Style, marshaller *internal.Marshaller) ([]byte, error) {
	output, err := marshaller.Marshal(event.Value, event.Timestamp)
	if err != nil {
		return nil, err
	}

	if c.searchQuery != nil {
		matches := c.searchQuery.FindAll(output, -1)
		if (matches != nil) == c.reverse {
			return nil, nil
		}
		for _, match := range matches {
			if highlightColor != nil {
				output = bytes.ReplaceAll(output, match, []byte(highlightColor.Sprint(string(match))))
			}
		}
	}

	return output, nil
}
