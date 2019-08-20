package main

import (
	"context"
	"crypto/tls"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strings"
	"sync"
	"syscall"

	"github.com/golang/protobuf/proto"
	"github.com/gookit/color"
	"github.com/pkg/errors"
	"github.com/pkg/profile"
	"github.com/xitonix/flags/core"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

var version string

func main() {

	initFlags()

	if versionRequest.Get() {
		printVersion()
		return
	}

	if internal.IsEmpty(environment.Get()) {
		exit(errors.New("The environment cannot be empty."))
	}

	var searchExpression *regexp.Regexp
	if searchQuery.IsSet() {
		se, err := regexp.Compile(searchQuery.Get())
		if err != nil {
			exit(errors.Wrap(err, "Failed to parse the search query"))
		}
		searchExpression = se
	}

	if profilingMode.IsSet() {
		switch strings.ToLower(profilingMode.Get()) {
		case "cpu":
			defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
		case "mem":
			defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
		case "mutex":
			defer profile.Start(profile.MutexProfile, profile.ProfilePath(".")).Stop()
		case "block":
			defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
		case "thread":
			defer profile.Start(profile.ThreadcreationProfile, profile.ProfilePath(".")).Stop()
		}
	}

	colorMode := strings.ToLower(terminalMode.Get())
	logFile, writeLogToFile, err := getLogWriter(logFilePath)
	if err != nil {
		exit(err)
	}

	theme := getColorTheme(colorMode, writeLogToFile)

	prn := internal.NewPrinter(internal.ToVerbosityLevel(verbosity.Get()), logFile, theme)

	loader, err := protobuf.NewFileLoader(protoDir.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}

	var tlsConfig *tls.Config
	if enableTLS.Get() {
		tlsConfig, err = configureTLS()
		if err != nil {
			exit(err)
		}
	}

	consumer, err := kafka.NewConsumer(
		brokers.Get(), prn,
		environment.Get(),
		enableAutoTopicCreation.Get(),
		kafka.WithClusterVersion(kafkaVersion.Get()),
		kafka.WithTLS(tlsConfig),
		kafka.WithSASL(saslMechanism.Get(), saslUsername.Get(), saslPassword.Get()))

	if err != nil {
		exit(err)
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

	topics := make(map[string]*kafka.Checkpoint)
	tm := make(map[string]string)
	cp := getCheckpoint(rewind.Get(), timeCheckpoint, offsetCheckpoint)
	if interactive.Get() {
		topics, tm, err = readUserData(consumer, loader, topicFilter.Get(), typeFilter.Get(), cp)
		if err != nil {
			exit(err)
		}
	} else {
		tm[topic.Get()] = messageType.Get()
		topics = getTopics(tm, cp)
	}

	for _, messageType := range tm {
		err := loader.Load(messageType)
		if err != nil {
			exit(err)
		}
	}

	writers, writeEventsToFile, err := getOutputWriters(outputDir, topics)
	if err != nil {
		exit(err)
	}

	prn.Start(writers)

	wg := sync.WaitGroup{}

	if len(tm) > 0 {
		wg.Add(1)
		consumerCtx, stopConsumer := context.WithCancel(context.Background())
		defer stopConsumer()
		go func() {
			defer wg.Done()
			reversed := reverse.Get()
			marshaller := protobuf.NewMarshaller(format.Get(), includeTimeStamp.Get())
			var searchColor color.Style
			if !writeEventsToFile {
				searchColor = getSearchColor(colorMode)
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
					output, err := process(tm[event.Topic], loader, event, marshaller, searchExpression, reversed, searchColor)
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
	} else {
		prn.Warning(internal.Forced, "Nothing to process. Terminating Trubka.")
	}

	// We still need to explicitly close the underlying Kafka client, in case `consumer.Start` has not been called.
	// It is safe to close the consumer twice.
	consumer.Close()
	wg.Wait()

	if err != nil {
		exit(err)
	}

	if writeLogToFile {
		closeFile(logFile.(*os.File))
	}

	if writeEventsToFile {
		for _, w := range writers {
			closeFile(w.(*os.File))
		}
	}
	prn.Info(internal.Verbose, "Trubka has been terminated successfully.")
	prn.Close()
}

func getCheckpoint(rewind bool, timeCheckpoint *core.TimeFlag, offsetCheckpoint *core.Int64Flag) *kafka.Checkpoint {
	cp := kafka.NewCheckpoint(rewind)
	switch {
	case offsetCheckpoint.IsSet():
		cp.SetOffset(offsetCheckpoint.Get())
	case timeCheckpoint.IsSet():
		cp.SetTimeOffset(timeCheckpoint.Get())
	}
	return cp
}

func printVersion() {
	if version == "" {
		version = "[built from source]"
	}
	fmt.Printf("Trubka %s\n", version)
}

func process(messageType string,
	loader *protobuf.FileLoader,
	event *kafka.Event,
	marshaller *protobuf.Marshaller,
	search *regexp.Regexp,
	reverse bool,
	highlightColor color.Style) ([]byte, error) {

	msg, err := loader.Get(messageType)
	if err != nil {
		return nil, err
	}

	err = proto.Unmarshal(event.Value, msg)
	if err != nil {
		return nil, err
	}

	output, err := marshaller.Marshal(msg, event.Timestamp)
	if err != nil {
		return nil, err
	}

	if search != nil {
		match := search.Find(output)
		if (match != nil) == reverse {
			return nil, nil
		}
		if highlightColor != nil {
			output = search.ReplaceAll(output, []byte(highlightColor.Sprint(string(match))))
		}
	}

	return output, nil
}

func getTopics(topicMap map[string]string, cp *kafka.Checkpoint) map[string]*kafka.Checkpoint {
	topics := make(map[string]*kafka.Checkpoint)
	for topic := range topicMap {
		topics[topic] = cp
	}
	return topics
}

func exit(err error) {
	color.Error.Printf("FATAL: %s\n", err)
	os.Exit(1)
}

func closeFile(file *os.File) {
	err := file.Sync()
	if err != nil {
		fmt.Printf("Failed to sync the file: %s\n", err)
	}
	if err := file.Close(); err != nil {
		fmt.Printf("Failed to close the file: %s\n", err)
	}
}
