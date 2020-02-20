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
	"github.com/xitonix/trubka/kafka"
)

type consumePlain struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters

	topic                   string
	decodeFrom              string
	encodeTo                string
	outputDir               string
	environment             string
	logFile                 string
	searchQuery             *regexp.Regexp
	topicFilter             *regexp.Regexp
	interactive             bool
	interactiveWithOffset   bool
	reverse                 bool
	includeTimestamp        bool
	includeKey              bool
	includeTopicName        bool
	enableAutoTopicCreation bool
	from                    string
	count                   bool
	highlightStyle          string
}

func addConsumePlainCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &consumePlain{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("plain", "Starts consuming plain text or json events from the given Kafka topic.").Action(cmd.run)
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

	c.Flag("decode-from", "The encoding of the incoming message content.").
		Short('D').
		Default(internal.PlainTextEncoding).
		EnumVar(&cmd.decodeFrom, internal.PlainTextEncoding, internal.Base64Encoding, internal.HexEncoding)

	c.Flag("format", "The format in which the incoming Kafka messages will be written to the output.").
		Default(internal.PlainTextEncoding).
		Short('F').
		EnumVar(&cmd.encodeTo,
			internal.PlainTextEncoding,
			internal.JsonIndentEncoding,
			internal.Base64Encoding,
			internal.HexEncoding)
}

func (c *consumePlain) run(_ *kingpin.ParseContext) error {
	interactive := c.interactive || c.interactiveWithOffset
	if !interactive && internal.IsEmpty(c.topic) {
		return errors.New("which Kafka topic you would like to consume from? Make sure you provide the topic as the first argument or switch to interactive mode (-i)")
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

	defaultCheckpoint, err := kafka.NewPartitionCheckpoints(c.from)
	if err != nil {
		return err
	}

	topics := make(map[string]*kafka.PartitionCheckpoints)

	if interactive {
		topics, err = askUserForTopics(consumer, c.topicFilter, c.interactiveWithOffset, defaultCheckpoint)
		if err != nil {
			return filterError(err)
		}
	} else {
		topics[c.topic] = defaultCheckpoint
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
	counter := internal.NewCounter()

	go func() {
		defer wg.Done()

		marshaller := internal.NewPlainTextMarshaller(c.decodeFrom,
			c.encodeTo,
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
				output, err := c.process(event, marshaller, c.globalParams.EnableColor && !writeEventsToFile)
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
		counter.PrintAsTable(c.globalParams.EnableColor)
	}

	return nil
}

func (c *consumePlain) process(event *kafka.Event, marshaller *internal.PlainTextMarshaller, highlight bool) ([]byte, error) {
	output, err := marshaller.Marshal(event.Value, event.Key, event.Timestamp, event.Topic, event.Partition)
	if err != nil {
		return nil, fmt.Errorf("invalid '%s' message received from Kafka: %w", c.decodeFrom, err)
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
