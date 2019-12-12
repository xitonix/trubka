package main

import (
	"fmt"
	"os"

	"github.com/xitonix/trubka/internal"
)

// Version build flags
var (
	version    string
	commit     string
	runtimeVer string
	built      string
)

var enabledColor bool

func main() {
	err := newApplication()
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	msg := fmt.Sprintf("ERROR: %s.", internal.Title(err))
	fmt.Println(internal.Err(msg, enabledColor))
	os.Exit(1)
}
