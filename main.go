package main

import (
	"fmt"
	"os"

	"github.com/jhump/protoreflect/dynamic"

	"go.xitonix.io/trubka/proto"
)

func main() {
	loader, err := proto.NewFileLoader("")
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
