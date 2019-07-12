package main

import (
	"fmt"
	"os"

	"github.com/xitonix/flags"

	"go.xitonix.io/trubka/internal"
	"go.xitonix.io/trubka/proto"
)

func main() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	protoDir := flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("p")
	protoFiles := flags.StringSlice("proto-files", `An optional list of the proto files to load. If not specified all the files in --proto-root will be processed.`).
		WithShort("F").
		WithTrimming()
	_ = flags.StringMap("topic-map", `The topic:message map (Example: -T {"topic-A":"namespace.MessageTypeA"})`).WithShort("T").Required()

	_ = flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").
		WithShort("k").
		Required().
		WithTrimming()

	v := flags.Verbosity("The verbosity level of the tool.")
	flags.Parse()
	prn := internal.NewPrinter(internal.ToVerbosityLevel(v.Get()))

	prn.Writef(internal.Verbose, "")

	loader, err := proto.NewFileLoader(protoDir.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}
	md, err := loader.Load("cm.protobuf.AdministratorDefined")
	if err != nil {
		exit(err)
	}

	fmt.Println(md)
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
