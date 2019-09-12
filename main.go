package main

import (
	"os"

	"github.com/gookit/color"
)

var version string

func main() {
	err := newApplication()
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	color.Error.Printf("FATAL: %s\n", err)
	os.Exit(1)
}
