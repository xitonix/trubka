package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"time"

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
	topicsMap := flags.StringMap("topic-map", `The topic:message map (Example: -T {"topic-A":"namespace.MessageTypeA"})`).WithShort("T").Required()

	brokers := flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").
		WithShort("k").
		Required().
		WithTrimming()

	kafkaVersion := flags.String("kafka-version", "Kafka cluster version.").WithDefault(kafka.DefaultClusterVersion)
	rewind := flags.Bool("rewind", "Read to beginning of the stream")

	v := flags.Verbosity("The verbosity level of the tool.")
	flags.Parse()
	prn := internal.NewPrinter(internal.ToVerbosityLevel(v.Get()))

	prn.Writef(internal.Verbose, "")

	loader, err := proto.NewFileLoader(protoDir.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}

	consumer, err := kafka.NewConsumer(brokers.Get(), prn, kafka.WithClusterVersion(kafkaVersion.Get()), kafka.WithRewind(rewind.Get()))
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

	err = consumer.Start(ctx, topics, func(topic string, partition int32, offset int64, time time.Time, key, value []byte) error {
		msg, err := loader.Load(tm[topic])
		if err != nil {
			return err
		}

		err = msg.Unmarshal(value)
		if err != nil {
			return err
		}

		json, err := msg.MarshalJSONIndent()
		if err != nil {
			return err
		}
		prn.Writeln(internal.Quiet, string(json))
		return nil
	})

	if err != nil {
		exit(err)
	}

	// md, err := loader.Load("cm.protobuf.AdministratorDefined")
	// if err != nil {
	// 	exit(err)
	// }

	// var originalBytes []byte
	// err = msg.Unmarshal(originalBytes)
	// if err != nil {
	// 	exit(err)
	// }
	//
	// j, err := msg.MarshalJSONIndent()
	// if err != nil {
	// 	exit(err)
	// }
	//
	// fmt.Println(string(j))
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
