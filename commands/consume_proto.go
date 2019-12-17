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
	reverse                 bool
	includeTimestamp        bool
	enableAutoTopicCreation bool
	count                   bool
	from                    string
}

func addConsumeProtoCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &consumeProto{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("proto", "Starts consuming protobuf encoded events from the given Kafka topic.").Action(cmd.run)
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
	cmd.bindCommandFlags(c)
}

func (c *consumeProto) bindCommandFlags(command *kingpin.CmdClause) {

	command.Arg("proto", "The fully qualified name of the protocol buffers type, stored in the given topic.").
		StringVar(&c.messageType)
	command.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		ExistingDirVar(&c.protoRoot)

	command.Flag("proto-filter", "The optional regular expression to filter the proto types by (Interactive mode only).").
		Short('p').
		RegexpVar(&c.protoFilter)
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
	checkpoints, err := kafka.NewPartitionCheckpoints(c.from)
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
