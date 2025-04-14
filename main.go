package main

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output/format"
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
	if err != nil && !errors.Is(err, context.Canceled) {
		exit(err)
	}
}

func exit(err error) {
	msg := fmt.Sprintf("ERROR: %s", internal.Title(err))
	msg = fmt.Sprint(format.Red(msg, enabledColor))
	logger := internal.NewLogger(1)
	logger.Log(1, msg)
	os.Exit(1)
}
