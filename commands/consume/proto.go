package consume

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sync"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

type consumeProto struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters

	protoRoot               string
	topic                   string
	messageType             string
	encodeTo                string
	outputDir               string
	environment             string
	logFile                 string
	topicFilter             *regexp.Regexp
	protoFilter             *regexp.Regexp
	searchQuery             *regexp.Regexp
	interactive             bool
	interactiveWithOffset   bool
	reverse                 bool
	includeTimestamp        bool
	includeKey              bool
	includeTopicName        bool
	enableAutoTopicCreation bool
	count                   bool
	from                    string
	highlightStyle          string
}

func addConsumeProtoCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &consumeProto{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("proto", "Starts consuming protobuf encoded events from the given Kafka topic.").Action(cmd.run)
	bindCommonConsumeFlags(c,
		&cmd.topic,
		&cmd.environment,
		&cmd.outputDir,
		&cmd.logFile,
		&cmd.from,
		&cmd.includeTimestamp,
		&cmd.includeKey,
		&cmd.includeTopicName,
		&cmd.enableAutoTopicCreation,
		&cmd.reverse,
		&cmd.interactive,
		&cmd.interactiveWithOffset,
		&cmd.count,
		&cmd.searchQuery,
		&cmd.topicFilter,
		&cmd.highlightStyle)

	cmd.bindCommandFlags(c)
}

func (c *consumeProto) bindCommandFlags(command *kingpin.CmdClause) {
	command.Arg("contract", "The fully qualified name of the protocol buffers type, stored in the given topic. The default value is the same as the topic name.").
		StringVar(&c.messageType)
	command.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&c.protoRoot)

	command.Flag("proto-filter", "The optional regular expression to filter the proto types by (Interactive mode only).").
		Short('p').
		RegexpVar(&c.protoFilter)

	command.Flag("format", "The format in which the incoming Kafka messages will be written to the output.").
		Default(internal.JsonIndentEncoding).
		Short('f').
		EnumVar(&c.encodeTo,
			internal.PlainTextEncoding,
			internal.JsonEncoding,
			internal.JsonIndentEncoding,
			internal.Base64Encoding,
			internal.HexEncoding)
}

func (c *consumeProto) run(_ *kingpin.ParseContext) error {
	interactive := c.interactive || c.interactiveWithOffset
	var implicitContract bool
	if !interactive {
		if internal.IsEmpty(c.topic) {
			return errors.New("which Kafka topic you would like to consume from? Make sure you provide the topic as the first argument or switch to interactive mode (-i)")
		}
		if internal.IsEmpty(c.messageType) {
			c.messageType = c.topic
			implicitContract = true
		}
	}

	logFile, writeLogToFile, err := getLogWriter(c.logFile)
	if err != nil {
		return err
	}

	prn := internal.NewPrinter(c.globalParams.Verbosity, logFile)

	consumer, err := initialiseConsumer(c.kafkaParams, c.globalParams, c.environment, c.enableAutoTopicCreation, logFile, prn)
	if err != nil {
		return err
	}

	// It is safe to close the consumer more than once.
	defer consumer.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go monitorCancellation(prn, cancel)

	tm := make(map[string]string)
	checkpoints, err := kafka.NewPartitionCheckpoints(c.from)
	if err != nil {
		return err
	}

	loader, err := protobuf.LoadFiles(ctx, c.globalParams.Verbosity, c.protoRoot)
	if err != nil {
		return err
	}

	var topics map[string]*kafka.PartitionCheckpoints
	if interactive {
		topics, tm, err = readUserData(consumer, loader, c.topicFilter, c.protoFilter, c.interactiveWithOffset, checkpoints)
		if err != nil {
			return filterError(err)
		}
	} else {
		tm[c.topic] = c.messageType
		topics = getTopics(tm, checkpoints)
	}

	for _, messageType := range tm {
		err := loader.Load(ctx, messageType)
		if err != nil {
			if implicitContract && !interactive {
				msg := "Most likely the message type is not exactly the same as the topic name."
				msg += "You may need to explicitly specify the fully qualified type name as the second argument to the consume command"
				msg += fmt.Sprintf("\nExample: trubka consume proto <flags...> %s <fully qualified type name>", c.topic)
				return fmt.Errorf("%w. %s", err, msg)
			}
			return err
		}
	}

	writers, writeEventsToFile, err := getOutputWriters(c.outputDir, topics)
	if err != nil {
		return err
	}

	prn.Start(writers)

	wg := sync.WaitGroup{}

	counter := internal.NewCounter()

	if len(tm) > 0 {
		wg.Add(1)
		consumerCtx, stopConsumer := context.WithCancel(context.Background())
		defer stopConsumer()
		go func() {
			defer wg.Done()

			marshaller := protobuf.NewMarshaller(c.encodeTo,
				c.includeTimestamp,
				c.includeTopicName && !writeEventsToFile,
				c.includeKey,
				c.globalParams.EnableColor && !writeEventsToFile,
				c.highlightStyle)

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
							counter.IncrSuccess(event.Topic)
						}
						continue
					}

					if c.count {
						counter.IncrFailure(event.Topic)
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
		prn.Infof(internal.SuperVerbose, "Closing the log file %s", c.logFile)
		closeFile(logFile.(*os.File), c.globalParams.EnableColor)
	}

	if writeEventsToFile {
		prn.Info(internal.SuperVerbose, "Closing the output files")
		for _, w := range writers {
			closeFile(w.(*os.File), c.globalParams.EnableColor)
		}
	}
	prn.Close()

	if c.count {
		counter.PrintAsTable(c.globalParams.EnableColor)
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

	output, err := marshaller.Marshal(msg, event.Key, event.Timestamp, event.Topic, event.Partition)
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
				output = bytes.ReplaceAll(output, match, []byte(fmt.Sprint(format.Yellow(string(match), true))))
			}
		}
	}

	return output, nil
}
