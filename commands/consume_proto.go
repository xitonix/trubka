package commands

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"sync"
	"syscall"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

type consumeProto struct {
	globalParams *GlobalParameters
	kafkaParams  *kafkaParameters

	protoRoot               string
	topic                   string
	messageType             string
	format                  string
	outputDir               string
	environment             string
	logFile                 string
	topicFilter             *regexp.Regexp
	protoFilter             *regexp.Regexp
	searchQuery             *regexp.Regexp
	interactive             bool
	rewind                  bool
	reverse                 bool
	includeTimestamp        bool
	enableAutoTopicCreation bool
	timeCheckpoint          time.Time
	offsetCheckpoints       []string
	count                   bool
}

func addConsumeProtoCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &consumeProto{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("proto", "Starts consuming protobuf encoded events from the given Kafka topic.").Action(cmd.run)
	cmd.bindCommandFlags(c)
}

func (c *consumeProto) bindCommandFlags(command *kingpin.CmdClause) {
	command.Arg("topic", "The Kafka topic to consume from.").StringVar(&c.topic)
	command.Arg("proto", "The fully qualified name of the protocol buffers type, stored in the given topic.").
		StringVar(&c.messageType)
	command.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		ExistingDirVar(&c.protoRoot)
	command.Flag("rewind", "Starts consuming from the beginning of the stream.").Short('w').BoolVar(&c.rewind)
	var timeCheckpoint string
	command.Flag("from", "Starts consuming from the most recent available offset at the given time. This will override --rewind.").
		Short('f').
		PreAction(func(context *kingpin.ParseContext) error {
			t, err := parseTime(timeCheckpoint)
			if err != nil {
				return err
			}
			c.timeCheckpoint = t
			return nil
		}).
		StringVar(&timeCheckpoint)
	command.Flag("from-offsets", `Starts consuming from the specified offset (if applicable). This will override --rewind and --from.
							If the most recent offset value of a partition is less than the specified value, this flag will be ignored.`).
		Short('o').
		StringsVar(&c.offsetCheckpoints)
	command.Flag("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").
		Short('T').
		BoolVar(&c.includeTimestamp)
	command.Flag("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
							Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`).
		BoolVar(&c.enableAutoTopicCreation)

	command.Flag("format", "The format in which the Kafka messages will be written to the output.").
		Default(internal.JsonIndent).
		EnumVar(&c.format,
			internal.Json,
			internal.JsonIndent,
			internal.Text,
			internal.TextIndent,
			internal.Hex,
			internal.HexIndent)
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

	// Interactive mode flags
	command.Flag("interactive", "Runs the consumer in interactive mode.").
		Short('i').
		BoolVar(&c.interactive)

	command.Flag("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").
		Short('t').
		RegexpVar(&c.topicFilter)

	command.Flag("proto-filter", "The optional regular expression to filter the proto types by (Interactive mode only).").
		Short('p').
		RegexpVar(&c.protoFilter)

	command.Flag("count", "Count the number of messages consumed from Kafka.").
		Short('c').
		BoolVar(&c.count)
}

func (c *consumeProto) run(_ *kingpin.ParseContext) error {
	if !c.interactive {
		if internal.IsEmpty(c.topic) {
			return errors.New("which Kafka topic you would like to consume from? Make sure you provide the topic as the first argument or switch to interactive mode (-i)")
		}
		if internal.IsEmpty(c.messageType) {
			return fmt.Errorf("which message type is stored in %s topic? Make sure you provide the fully qualified proto name as the second argument or switch to interactive mode (-i)", c.topic)
		}
	}
	logFile, writeLogToFile, err := getLogWriter(c.logFile)
	if err != nil {
		return err
	}

	prn := internal.NewPrinter(c.globalParams.Verbosity, logFile)

	loader, err := protobuf.NewFileLoader(c.protoRoot)
	if err != nil {
		return err
	}

	saramaLogWriter := ioutil.Discard
	if c.globalParams.Verbosity >= internal.Chatty {
		saramaLogWriter = logFile
	}

	consumer, err := c.kafkaParams.createConsumer(prn, c.environment, c.enableAutoTopicCreation, saramaLogWriter)
	if err != nil {
		return err
	}

	// It is safe to close the consumer more than once.
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		prn.Info(internal.Verbose, "Stopping Trubka.")
		cancel()
	}()

	topics := make(map[string]*kafka.PartitionCheckpoints)
	tm := make(map[string]string)
	checkpoints, err := getCheckpoints(c.rewind, c.offsetCheckpoints, c.timeCheckpoint)
	if err != nil {
		return err
	}
	if c.interactive {
		topics, tm, err = readUserData(consumer, loader, c.topicFilter, c.protoFilter, checkpoints)
		if err != nil {
			return err
		}
	} else {
		tm[c.topic] = c.messageType
		topics = getTopics(tm, checkpoints)
	}

	for _, messageType := range tm {
		err := loader.Load(messageType)
		if err != nil {
			return err
		}
	}

	writers, writeEventsToFile, err := getOutputWriters(c.outputDir, topics)
	if err != nil {
		return err
	}

	prn.Start(writers)

	wg := sync.WaitGroup{}

	counter := &internal.Counter{}

	if len(tm) > 0 {
		wg.Add(1)
		consumerCtx, stopConsumer := context.WithCancel(context.Background())
		defer stopConsumer()
		go func() {
			defer wg.Done()
			marshaller := protobuf.NewMarshaller(c.format, c.includeTimestamp)
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

					output, err := c.process(tm[event.Topic], loader, event, marshaller, c.globalParams.EnableColor && !writeEventsToFile)
					if err == nil {
						prn.WriteEvent(event.Topic, output)
						consumer.StoreOffset(event)
						if c.count {
							counter.IncrSuccess()
						}
						continue
					}

					if c.count {
						counter.IncrFailure()
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
	} else {
		prn.Warning(internal.Forced, "Nothing to process. Terminating Trubka.")
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
		closeFile(logFile.(*os.File), c.globalParams.EnableColor)
	}

	if writeEventsToFile {
		for _, w := range writers {
			closeFile(w.(*os.File), c.globalParams.EnableColor)
		}
	}
	prn.Close()

	if c.count {
		counter.Print(c.globalParams.EnableColor)
	}

	return nil
}

func (c *consumeProto) process(messageType string,
	loader *protobuf.FileLoader,
	event *kafka.Event,
	marshaller *protobuf.Marshaller,
	highlight bool) ([]byte, error) {

	msg, err := loader.Get(messageType)
	if err != nil {
		return nil, err
	}

	err = msg.Unmarshal(event.Value)
	if err != nil {
		return nil, err
	}

	output, err := marshaller.Marshal(msg, event.Timestamp)
	if err != nil {
		return nil, err
	}

	if c.searchQuery != nil {
		matches := c.searchQuery.FindAll(output, -1)
		if (matches != nil) == c.reverse {
			return nil, nil
		}
		for _, match := range matches {
			if highlight {
				output = bytes.ReplaceAll(output, match, []byte(fmt.Sprint(internal.Yellow(string(match), true))))
			}
		}
	}

	return output, nil
}
