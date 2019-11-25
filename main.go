package main

import (
	"fmt"
	"os"
)

var version string

func main() {
	err := newApplication()
	if err != nil {
		exit(err)
	}
}

func exit(err error) {
	fmt.Printf("FATAL: %s\n", err)
	os.Exit(1)
}
