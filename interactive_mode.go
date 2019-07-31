package main

import (
	"bufio"
	"context"
	"fmt"
	"io"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/proto"
)

type confirmationResult int8

const (
	yes confirmationResult = iota
	no
	quit
)

func readUserData(ctx context.Context, consumer *kafka.Consumer, loader proto.Loader, topicFilter string, typeFilter string) ([]string, map[string]string, error) {
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
	topics := make([]string, 0)
	proceed := repeatUntilNo(ctx, "More topics to consume from", func() bool {
		topicIndex := pickAnIndex("Choose the topic to consume from", "topic", remoteTopic)
		if topicIndex < 0 {
			return false
		}
		topic := remoteTopic[topicIndex]
		typeIndex := pickAnIndex(fmt.Sprintf("Select the message type stored in %s", topic), "message type", types)
		if typeIndex < 0 {
			return false
		}
		tm[topic] = types[typeIndex]
		topics = append(topics, topic)
		return true
	})
	if !proceed {
		return nil, nil, nil
	}
	return topics, tm, nil
}

// repeatUntilNo repeats an action until the user stops
func repeatUntilNo(ctx context.Context, message string, action func() bool) bool {
	for {
		if !action() {
			return false
		}
		result := askForConfirmationWithCancellation(ctx, message)
		switch result {
		case quit:
			return false
		case no:
			return true
		default:
			continue
		}
	}
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

// askForConfirmationWithCancellation asks the user for confirmation. The user must type in "yes/y", "no/n" or "exit/quit/q"
// and then press enter. It has fuzzy matching, so "y", "Y", "yes", "YES", and "Yes" all count as
// confirmations. If the input is not recognized, it will ask again. The function does not return
// until it gets a valid response from the user or receives a cancellation signal.
func askForConfirmationWithCancellation(ctx context.Context, s string) confirmationResult {

	input := make(chan confirmationResult)

	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		msg := fmt.Sprintf("%s [y/n/q]?: ", s)
		for fmt.Print(msg); scanner.Scan(); fmt.Print(msg) {
			r := strings.ToLower(strings.TrimSpace(scanner.Text()))
			switch r {
			case "y", "yes":
				input <- yes
				return
			case "n", "no":
				input <- no
				return
			case "q", "quit", "exit":
				input <- quit
				return
			}
		}
	}()

	for {
		select {
		case <-ctx.Done():
			return quit
		case response := <-input:
			return response
		}
	}

	return quit
}
