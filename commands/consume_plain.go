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

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
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
	interactive             bool
	topicFilter             *regexp.Regexp
	reverse                 bool
	includeTimestamp        bool
	enableAutoTopicCreation bool
	from                    string
	count                   bool
}

func addConsumePlainCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &consumePlain{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("plain", "Starts consuming plain text or json events from the given Kafka topic.").Action(cmd.run)
	bindCommonConsumeFlags(c,
		&cmd.topic,
		&cmd.format,
		&cmd.environment,
		&cmd.outputDir,
		&cmd.logFile,
		&cmd.from,
		&cmd.includeTimestamp,
		&cmd.enableAutoTopicCreation,
		&cmd.reverse,
		&cmd.interactive,
		&cmd.count,
		&cmd.searchQuery,
		&cmd.topicFilter)
}

func (c *consumePlain) run(_ *kingpin.ParseContext) error {
	if !c.interactive && internal.IsEmpty(c.topic) {
		return errors.New("which Kafka topic you would like to consume from? Make sure you provide the topic as the first argument or switch to interactive mode (-i)")
	}

	logFile, writeLogToFile, err := getLogWriter(c.logFile)
	if err != nil {
		return err
	}

	prn := internal.NewPrinter(c.globalParams.Verbosity, logFile)

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

	if c.interactive {
		c.topic, err = askUserForTopic(consumer, c.topicFilter)
		if err != nil {
			return err
		}
	}

	if internal.IsEmpty(c.topic) {
		return nil
	}

	checkpoints, err := kafka.NewPartitionCheckpoints(c.from)
	if err != nil {
		return err
	}
	topics := map[string]*kafka.PartitionCheckpoints{
		c.topic: checkpoints,
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
	counter := &internal.Counter{}

	go func() {
		defer wg.Done()
		marshaller := internal.NewPlainTextMarshaller(c.format, c.includeTimestamp)

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

func (c *consumePlain) process(event *kafka.Event, marshaller *internal.Marshaller, highlight bool) ([]byte, error) {
	output, err := marshaller.Marshal(event.Value, event.Timestamp)
	if err != nil {
		return nil, fmt.Errorf("invalid '%s' message received from Kafka: %w", c.format, err)
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
