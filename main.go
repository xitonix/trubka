package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/xitonix/flags"

	"go.xitonix.io/trubka/internal"
	"go.xitonix.io/trubka/kafka"
	"go.xitonix.io/trubka/proto"
)

func main() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	protoDir := flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("p")
	protoFiles := flags.StringSlice("proto-files", `An optional list of the proto files to load. If not specified all the files in --proto-root will be processed.`).
		WithShort("F").
		WithTrimming()
	topicsMap := flags.StringMap("topic-map", `Specifies the mappings between topics and message types in '{"Topic_Name":"Fully_Qualified_Message_Type"}' format.
						Example: --topic-map '{"CPU":"contracts.CPUStatusChanged", "RAM":"contracts.MemoryUsageChanged"}'.`).
		WithShort("t").
		Required()

	brokers := flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").
		WithShort("k").
		Required().
		WithTrimming()

	format := flags.String("format", "The format in which the Kafka messages will be written to the specified output.").
		WithValidRange(true, "json", "json-indent", "text", "text-indent", "hex", "hex-indent").WithDefault("json-indent")

	kafkaVersion := flags.String("kafka-version", "Kafka cluster version.").WithDefault(kafka.DefaultClusterVersion)
	rewind := flags.Bool("rewind", "Read to beginning of the stream")
	resetOffsets := flags.Bool("reset-offsets", "Resets the stored offsets").WithShort("r")
	v := flags.Verbosity("The verbosity level of the tool.")

	flags.Parse()

	prn := internal.NewPrinter(internal.ToVerbosityLevel(v.Get()))

	loader, err := proto.NewFileLoader(protoDir.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}

	consumer, err := kafka.NewConsumer(
		brokers.Get(),
		prn,
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

	tm := topicsMap.Get()
	topics := make([]string, 0)
	for topic := range tm {
		topics = append(topics, topic)
	}

	var marshal func(msg *dynamic.Message) ([]byte, error)

	marshal = getMarshaller(format.Get())

	err = consumer.Start(ctx, topics, func(topic string, partition int32, offset int64, time time.Time, key, value []byte) error {
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

		output, err := marshal(msg)
		if err != nil {
			return err
		}
		prn.Writeln(internal.Quiet, string(output))
		return nil
	})

	if err != nil {
		exit(err)
	}
}

func getMarshaller(format string) func(msg *dynamic.Message) ([]byte, error) {
	f := strings.TrimSpace(strings.ToLower(format))
	switch f {
	case "hex", "hex-indent":
		return func(msg *dynamic.Message) ([]byte, error) {
			output, err := msg.Marshal()
			if err != nil {
				return nil, err
			}
			fm := "%X"
			if f == "hex-indent" {
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

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
