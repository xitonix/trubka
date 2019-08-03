package main

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/pkg/errors"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/proto"
)

func readUserData(consumer *kafka.Consumer, loader proto.Loader, topicFilter string, typeFilter string, cp *kafka.Checkpoint) (map[string]*kafka.Checkpoint, map[string]string, error) {
	remoteTopic, err := consumer.GetTopics(topicFilter)
	if err != nil {
		return nil, nil, err
	}
	types, err := loader.List(typeFilter)
	if err != nil {
		return nil, nil, err
	}

	sort.Strings(remoteTopic)
	sort.Strings(types)

	tm := make(map[string]string)
	topics := make(map[string]*kafka.Checkpoint, 0)
	topicIndex := pickAnIndex("Choose the topic to consume from", "topic", remoteTopic)
	if topicIndex < 0 {
		return nil, nil, nil
	}
	topic := remoteTopic[topicIndex]
	typeIndex := pickAnIndex(fmt.Sprintf("Select the message type stored in %s", topic), "message type", types)
	if typeIndex < 0 {
		return nil, nil, nil
	}
	tm[topic] = types[typeIndex]

	switch cp.Mode() {
	case kafka.ExplicitOffsetMode:
		msg := fmt.Sprintf("Enter an offset to start consuming from or press ENTER to select %d", cp.Offset())
		offset, proceed := readInput(msg, "offset", func(input string) (interface{}, error) {
			if internal.IsEmpty(input) {
				return cp.Offset(), nil
			}
			result, err := strconv.ParseInt(input, 10, 64)
			if err != nil {
				return nil, errors.New("The offset must be a valid integer.")
			}
			return result, nil
		})
		if !proceed {
			return nil, nil, nil
		}
		cp.SetOffset(offset.(int64))
	case kafka.MillisecondsOffsetMode:
		msg := fmt.Sprintf("Enter a time to start consuming from or press ENTER to select %v", cp.TimeOffset())
		offset, proceed := readInput(msg, "time offset", func(input string) (interface{}, error) {
			if internal.IsEmpty(input) {
				return cp.TimeOffset(), nil
			}
			result, err := parseTime(input)
			if err != nil {
				return nil, err
			}
			return result, nil
		})
		if !proceed {
			return nil, nil, nil
		}
		cp.SetTimeOffset(offset.(time.Time))
	}

	topics[topic] = cp

	proceed := askForConfirmation("Start consuming?")
	if !proceed {
		return nil, nil, nil
	}

	return topics, tm, nil
}

// pickAnIndex returns the index of one of the items within the list
func pickAnIndex(message, entryName string, input []string) int {
	if len(input) == 0 {
		fmt.Printf("No %ss found. You may need to tweak the %[1]s filter.\n", entryName)
		return -1
	}
	for i, t := range input {
		fmt.Printf("%2d: %v\n", i+1, t)
	}
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%s (Q to quit): ", message)
		inputStr, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Could not read the input. %s\n", err)
			continue
		}
		trimmed := strings.TrimSpace(inputStr)
		if len(trimmed) == 0 {
			fmt.Printf("One %s should be selected.\n", entryName)
			continue
		}

		if strings.EqualFold(trimmed, "Q") ||
			strings.EqualFold(trimmed, "Quit") ||
			strings.EqualFold(trimmed, "Exit") {
			return -1
		}

		i, err := strconv.Atoi(strings.TrimSpace(trimmed))
		if err != nil || i > len(input) || i < 1 {
			fmt.Printf("The selected %s index should be between 1 and %d\n", entryName, len(input))
			continue
		}
		return i - 1
	}
	return -1
}

func readInput(message, entryName string, parse func(input string) (interface{}, error)) (interface{}, bool) {
	reader := bufio.NewReader(os.Stdin)
	for {
		fmt.Printf("%s (Q to quit): ", message)
		inputStr, err := reader.ReadString('\n')
		if err != nil {
			if err == io.EOF {
				break
			}
			fmt.Printf("Could not read %s. %s\n", entryName, err)
			continue
		}
		trimmed := strings.TrimSpace(inputStr)

		if strings.EqualFold(trimmed, "Q") ||
			strings.EqualFold(trimmed, "Quit") ||
			strings.EqualFold(trimmed, "Exit") {
			return nil, false
		}

		result, err := parse(trimmed)
		if err != nil {
			fmt.Printf("Invalid %s value. %s\n", entryName, err)
			continue
		}
		return result, true
	}
	return nil, false
}

// askForConfirmation asks the user for confirmation. The user must type in "yes/y", "no/n" or "exit/quit/q"
// and then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user.
func askForConfirmation(s string) bool {
	scanner := bufio.NewScanner(os.Stdin)
	msg := fmt.Sprintf("%s [y/n]?: ", s)
	for fmt.Print(msg); scanner.Scan(); fmt.Print(msg) {
		r := strings.ToLower(strings.TrimSpace(scanner.Text()))
		switch r {
		case "y", "yes":
			return true
		case "n", "no":
			return false
		}
	}
	return false
}
