package main

import (
	"context"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"runtime"
	"runtime/pprof"
	"sort"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/xitonix/flags"
	"github.com/xitonix/flags/core"

	"go.xitonix.io/trubka/internal"
	"go.xitonix.io/trubka/kafka"
	"go.xitonix.io/trubka/proto"
)

var version string

type marshaller func(msg *dynamic.Message) ([]byte, error)

func main() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	cpuProfile := flags.String("cpu-profile", "Writes cpu profiles to `file`").Hide()
	memProfile := flags.String("mem-profile", "Writes memory profiles to `file`").Hide()
	protoDir := flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("p")
	protoFiles := flags.StringSlice("proto-files", `An optional list of the proto files to load. If not specified all the files in --proto-root will be processed.`)
	protoPrefix := flags.String("type-prefix", "The optional prefix to prepend to proto message types.").WithShort("m")
	topicsMap := flags.StringMap("topic-map", `Specifies the mappings between topics and message types in "Topic_Name:Fully_Qualified_Message_Type" format.
						Example: --topic-map "CPU:contracts.CPUStatusChanged, RAM:contracts.MemoryUsageChanged".`).WithShort("t")

	brokers := flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").WithShort("k").Required()
	topicPrefix := flags.String("kafka-prefix", "The optional prefix to add to Kafka topic names.").WithShort("s")
	enableAutoTopicCreation := flags.Bool("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
						Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`)

	format := flags.String("format", "The format in which the Kafka messages will be written to the output.").
		WithValidRange(true, "json", "json-indent", "text", "text-indent", "hex", "hex-indent").
		WithDefault("json-indent").WithShort("f")

	logFilePath := flags.String("log-file", "The `file` to write the logs to. Set to '' to discard (Default: stdout).").WithShort("l")
	outFilePath := flags.String("output-file", "The `file` to write the Kafka messages to. Set to '' to discard (Default: Stdout).").WithShort("u")
	kafkaVersion := flags.String("kafka-version", "Kafka cluster version.").WithDefault(kafka.DefaultClusterVersion)
	rewind := flags.Bool("rewind", "Read to beginning of the stream").WithShort("w")
	resetOffsets := flags.Bool("reset-offsets", "Resets the stored offsets").WithShort("r")
	environment := flags.String("environment", `This is to store the local offsets in different files for different environments. It's This is only required
						if you use trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).WithShort("E")
	interactive := flags.Bool("interactive", "Runs the tool in interactive mode.").WithShort("i")
	topicFilter := flags.String("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").WithShort("q")
	typeFilter := flags.String("type-filter", "The optional regular expression to filter the proto types with (Interactive mode only).").WithShort("y")
	v := flags.Verbosity("The verbosity level of the tool.")
	version := flags.Bool("version", "Prints the current version of Trubka.")

	flags.Parse()

	if version.Get() {
		printVersion()
		return
	}

	if cpuProfile.IsSet() {
		startCPUProfiling(cpuProfile.Get())
		defer pprof.StopCPUProfile()
	}

	if memProfile.IsSet() {
		startMemoryProfiling(memProfile.Get())
	}

	logFile, closableLog, err := getFile(logFilePath)
	if err != nil {
		exit(err)
	}

	outFile, closableOutput, err := getFile(outFilePath)
	if err != nil {
		exit(err)
	}

	prn := internal.NewPrinter(internal.ToVerbosityLevel(v.Get()), logFile, outFile)

	loader, err := proto.NewFileLoader(protoDir.Get(), protoPrefix.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}

	consumer, err := kafka.NewConsumer(
		brokers.Get(), prn,
		environment.Get(),
		enableAutoTopicCreation.Get(),
		kafka.WithClusterVersion(kafkaVersion.Get()),
		kafka.WithRewind(rewind.Get()),
		kafka.WithOffsetReset(resetOffsets.Get()))

	if err != nil {
		exit(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		cancel()
	}()

	var topics []string
	tm := make(map[string]string)
	if interactive.Get() {
		topics, tm, err = readUserData(ctx, consumer, loader, topicFilter.Get(), typeFilter.Get())
		if err != nil {
			exit(err)
		}
	} else {
		tm = topicsMap.Get()
		prefix := strings.TrimSpace(topicPrefix.Get())
		topics = getTopics(prefix, tm)
	}

	var marshal func(msg *dynamic.Message) ([]byte, error)
	marshal = getMarshaller(format.Get())

	if len(tm) > 0 {
		prn.Log(internal.Quiet, "Consuming from:")
		for t, m := range tm {
			prn.Logf(internal.Quiet, "    %s: %s", t, m)
		}
		err = consumer.Start(ctx, topics, func(topic string, partition int32, offset int64, time time.Time, key, value []byte) error {
			return consume(tm, topic, loader, value, marshal, prn)
		})
	} else {
		prn.Log(internal.Quiet, "No Kafka topic has been selected.")
	}

	consumer.Close()
	prn.Close()

	if err != nil {
		exit(err)
	}

	if closableLog {
		closeFile(logFile.(*os.File))
	}

	if closableOutput {
		closeFile(outFile.(*os.File))
	}
}

func printVersion() {
	if version == "" {
		version = "[build from source]"
	}
	fmt.Printf("Trubka %s\n", version)
}

func consume(tm map[string]string, topic string, loader *proto.FileLoader, value []byte, serialise marshaller, prn *internal.SyncPrinter) error {
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
	prn.WriteMessage(output)
	return nil
}

func startMemoryProfiling(profileFile string) {
	f, err := os.Create(profileFile)
	if err != nil {
		exit(err)
	}
	runtime.GC()
	if err = pprof.WriteHeapProfile(f); err != nil {
		exit(err)
	}
}

func startCPUProfiling(profileFile string) {
	f, err := os.Create(profileFile)
	if err != nil {
		exit(err)
	}
	err = pprof.StartCPUProfile(f)
	if err != nil {
		exit(err)
	}
}

func getTopics(prefix string, topicMap map[string]string) []string {
	topics := make([]string, 0)
	for topic := range topicMap {
		if len(prefix) > 0 && !strings.HasPrefix(topic, prefix) {
			topic = prefix + topic
		}
		topics = append(topics, topic)
	}
	sort.Strings(topics)
	return topics
}

func exit(err error) {
	fmt.Printf("FATAL: %s\n", err)
	os.Exit(1)
}

func getFile(f *core.StringFlag) (io.Writer, bool, error) {
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
