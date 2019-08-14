package main

import (
	"bufio"
	"fmt"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

func readUserData(consumer *kafka.Consumer, loader protobuf.Loader, topicFilter string, typeFilter string, cp *kafka.Checkpoint) (map[string]*kafka.Checkpoint, map[string]string, error) {
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

	var msg string
	switch cp.Mode() {
	case kafka.MillisecondsOffsetMode:
		msg = fmt.Sprintf("Start consuming from the closest offset available at %s of topic %s?", cp.OffsetString(), topic)
	case kafka.ExplicitOffsetMode:
		msg = fmt.Sprintf("Start consuming from offset %s of %s topic?", cp.OffsetString(), topic)
	default:
		msg = fmt.Sprintf("Start consuming from %s topic?", topic)
	}

	topics[topic] = cp

	proceed := askForConfirmation(msg)
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
	scanner := bufio.NewScanner(os.Stdin)
	msg := fmt.Sprintf("%s (Q to quit): ", message)
	for fmt.Print(msg); scanner.Scan(); fmt.Print(msg) {

		trimmed := strings.TrimSpace(scanner.Text())
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
