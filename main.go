package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/pkg/profile"
	"github.com/xitonix/flags"
	"github.com/xitonix/flags/core"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/proto"
)

var version string

type marshaller func(msg *dynamic.Message) ([]byte, error)

func main() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	profilingMode := flags.String("profile", "Enables profiling.").WithValidRange(true, "cpu", "mem", "block", "mutex").Hide()

	protoDir := flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("p")
	protoFiles := flags.StringSlice("proto-files", `An optional list of the proto files to load. If not specified all the files in --proto-root will be processed.`)
	protoPrefix := flags.String("type-prefix", "The optional prefix to prepend to proto message types.").WithShort("x")
	topicsMap := flags.StringMap("topic-map", `Specifies the mappings between topics and message types in "Topic_Name:Fully_Qualified_Message_Type" format.
						Example: --topic-map "CPU:contracts.CPUStatusChanged, RAM:contracts.MemoryUsageChanged".`).WithShort("t")

	brokers := flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").WithShort("k")
	topicPrefix := flags.String("kafka-prefix", "The optional prefix to add to Kafka topic names.").WithShort("s")
	enableAutoTopicCreation := flags.Bool("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
						Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`)

	format := flags.String("format", "The format in which the Kafka messages will be written to the output.").
		WithValidRange(true, "json", "json-indent", "text", "text-indent", "hex", "hex-indent").
		WithDefault("json-indent").WithShort("f")

	logFilePath := flags.String("log-file", "The `file` to write the logs to. Set to '' to discard (Default: stdout).").WithShort("l")
	outputDir := flags.String("output-dir", "The `directory` to write the Kafka messages to. Set to '' to discard (Default: Stdout).").WithShort("d")
	kafkaVersion := flags.String("kafka-version", "Kafka cluster version.").WithDefault(kafka.DefaultClusterVersion)
	rewind := flags.Bool("rewind", "Starts consuming from the beginning of the stream.").WithShort("w")
	timeCheckpoint := flags.Time("from-time", `Starts consuming from the most recent available offset at the given time. This will override --rewind.`)
	offsetCheckpoint := flags.Int64("from-offset", `Starts consuming from the specified offset (if applicable). This will override --rewind and --time-checkpoint.`)
	environment := flags.String("environment", `This is to store the local offsets in different files for different environments. It's This is only required
						if you use trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).WithShort("E").Required()
	interactive := flags.Bool("interactive", "Runs the tool in interactive mode.").WithShort("i")
	topicFilter := flags.String("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").WithShort("m")
	typeFilter := flags.String("type-filter", "The optional regular expression to filter the proto types with (Interactive mode only).").WithShort("n")
	searchQuery := flags.String("search-query", "The optional regular expression to filter the message content by.").WithShort("q")
	reverse := flags.Bool("reverse", "If set, the messages of which the content matches the search query will be ignored.")
	v := flags.Verbosity("The verbosity level of the tool.")
	version := flags.Bool("version", "Prints the current version of Trubka.")

	flags.Parse()

	if version.Get() {
		printVersion()
		return
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
		}
	}

	logFile, closableLog, err := getLogWriter(logFilePath)
	if err != nil {
		exit(err)
	}

	prn := internal.NewPrinter(internal.ToVerbosityLevel(v.Get()), logFile)

	loader, err := proto.NewFileLoader(protoDir.Get(), protoPrefix.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}

	consumer, err := kafka.NewConsumer(
		brokers.Get(), prn,
		environment.Get(),
		enableAutoTopicCreation.Get(),
		kafka.WithClusterVersion(kafkaVersion.Get()))

	if err != nil {
		exit(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		prn.Log(internal.Verbose, "Stopping Trubka.")
		cancel()
	}()

	topics := make(map[string]*kafka.Checkpoint)
	tm := make(map[string]string)
	cp := getCheckpoint(rewind.Get(), timeCheckpoint, offsetCheckpoint)
	if interactive.Get() {
		topics, tm, err = readUserData(ctx, consumer, loader, topicFilter.Get(), typeFilter.Get(), cp)
		if err != nil {
			exit(err)
		}
	} else {
		tm = topicsMap.Get()
		prefix := strings.TrimSpace(topicPrefix.Get())
		topics = getTopics(prefix, tm, cp)
	}

	writers, closable, err := getOutputWriters(outputDir, topics)
	if err != nil {
		exit(err)
	}

	prn.Start(writers)

	var marshal func(msg *dynamic.Message) ([]byte, error)
	marshal = getMarshaller(format.Get())

	if len(tm) > 0 {
		prn.Log(internal.Forced, "Consuming from:")
		for t, m := range tm {
			prn.Logf(internal.Forced, "    %s: %s", t, m)
		}
		reversed := reverse.Get()
		err = consumer.Start(ctx, topics, func(topic string, partition int32, offset int64, time time.Time, value []byte) error {
			return consume(tm, topic, loader, value, marshal, prn, searchExpression, reversed)
		})
	} else {
		prn.Log(internal.Forced, "No Kafka topic has been selected.")
	}

	// We still need to explicitly close the underlying Kafka client, in case `consumer.Start` has not been called.
	// It is safe to close the consumer twice.
	consumer.Close()

	prn.Close()

	if err != nil {
		exit(err)
	}

	if closableLog {
		closeFile(logFile.(*os.File))
	}

	if closable {
		for _, w := range writers {
			closeFile(w.(*os.File))
		}
	}
}

func getCheckpoint(rewind bool, timeCheckpoint *core.TimeFlag, offsetCheckpoint *core.Int64Flag) *kafka.Checkpoint {
	if offsetCheckpoint.IsSet() {
		return kafka.NewOffsetCheckpoint(offsetCheckpoint.Get())
	}
	if timeCheckpoint.IsSet() {
		return kafka.NewTimeCheckpoint(timeCheckpoint.Get())
	}
	return kafka.NewCheckpoint(rewind)
}

func printVersion() {
	if version == "" {
		version = "[build from source]"
	}
	fmt.Printf("Trubka %s\n", version)
}

func consume(tm map[string]string,
	topic string,
	loader *proto.FileLoader,
	value []byte,
	serialise marshaller,
	prn *internal.SyncPrinter,
	search *regexp.Regexp,
	reverse bool) error {
	messageType, ok := tm[topic]
	if !ok || internal.IsEmpty(messageType) {
		return errors.New("the message type cannot be empty")
	}
	msg, err := loader.Load(messageType)
	if err != nil {
		return err
	}
	err = msg.Unmarshal(value)
	if err != nil {
		return err
	}
	output, err := serialise(msg)
	if err != nil {
		return err
	}
	if search != nil && search.Match(output) == reverse {
		return nil
	}
	prn.WriteMessage(topic, output)
	return nil
}

func getTopics(prefix string, topicMap map[string]string, cp *kafka.Checkpoint) map[string]*kafka.Checkpoint {
	topics := make(map[string]*kafka.Checkpoint)
	for topic := range topicMap {
		if len(prefix) > 0 && !strings.HasPrefix(topic, prefix) {
			topic = prefix + topic
		}
		topics[topic] = cp
	}
	return topics
}

func exit(err error) {
	fmt.Printf("FATAL: %s\n", err)
	os.Exit(1)
}

func getLogWriter(f *core.StringFlag) (io.Writer, bool, error) {
	file := f.Get()
	if internal.IsEmpty(file) {
		if f.IsSet() {
			return ioutil.Discard, false, nil
		}
		return os.Stdout, false, nil
	}
	lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
	}
	return lf, true, nil
}

func getOutputWriters(dir *core.StringFlag, topics map[string]*kafka.Checkpoint) (map[string]io.Writer, bool, error) {
	root := dir.Get()
	result := make(map[string]io.Writer)

	if internal.IsEmpty(root) {
		discard := dir.IsSet()
		for topic := range topics {
			if discard {
				result[topic] = ioutil.Discard
				continue
			}
			result[topic] = os.Stdout
		}
		return result, false, nil
	}

	err := os.MkdirAll(root, 0755)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to create the output directory")
	}

	for topic := range topics {
		file := filepath.Join(root, topic)
		lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
		}
		result[topic] = lf
	}

	return result, true, nil
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

func getMarshaller(format string) marshaller {
	format = strings.TrimSpace(strings.ToLower(format))
	switch format {
	case "hex", "hex-indent":
		return func(msg *dynamic.Message) ([]byte, error) {
			output, err := msg.Marshal()
			if err != nil {
				return nil, err
			}
			fm := "%X"
			if format == "hex-indent" {
				fm = "% X"
			}
			return []byte(fmt.Sprintf(fm, output)), nil
		}
	case "text":
		return func(msg *dynamic.Message) ([]byte, error) {
			return msg.MarshalText()
		}
	case "text-indent":
		return func(msg *dynamic.Message) ([]byte, error) {
			return msg.MarshalTextIndent()
		}
	case "json":
		return func(msg *dynamic.Message) ([]byte, error) {
			return msg.MarshalJSON()
		}
	default:
		return func(msg *dynamic.Message) ([]byte, error) {
			return msg.MarshalJSONIndent()
		}
	}
}
