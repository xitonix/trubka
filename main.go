package main

import (
	"fmt"
	"os"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/xitonix/flags"

	"go.xitonix.io/trubka/proto"
)

func main() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	protoDir := flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("p")
	_ = flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").Required().WithTrimming()
	_ = flags.Verbosity("The verbosity level of the tool (use -vv for more verbosity).")
	flags.Parse()

	if protoDir.IsSet() {
		// TODO
	}
	loader, err := proto.NewFileLoader(protoDir.Get())
	if err != nil {
		exit(err)
	}
	md, err := loader.Find("")
	if err != nil {
		exit(err)
	}

	msg := dynamic.NewMessage(md)
	var originalBytes []byte
	err = msg.Unmarshal(originalBytes)
	if err != nil {
		exit(err)
	}

	j, err := msg.MarshalJSONIndent()
	if err != nil {
		exit(err)
	}

	fmt.Println(string(j))
}

func exit(err error) {
	fmt.Println(err)
	os.Exit(1)
}
