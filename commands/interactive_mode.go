package commands

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"regexp"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

var errExitInteractiveMode = errors.New("exit")

func askUserForTopics(consumer *kafka.Consumer,
	topicFilter *regexp.Regexp,
	offsetInteractiveMode bool,
	defaultCheckpoint *kafka.PartitionCheckpoints) (map[string]*kafka.PartitionCheckpoints, error) {

	remoteTopics, err := consumer.GetTopics(topicFilter)
	if err != nil {
		return nil, err
	}

	sort.Strings(remoteTopics)
	indexes, err := pickAnIndex("to consume from", "topic", remoteTopics, true)
	if err != nil {
		return nil, err
	}

	result := make(map[string]*kafka.PartitionCheckpoints)
	for _, index := range indexes {
		topic := remoteTopics[index]
		result[topic] = defaultCheckpoint
		if offsetInteractiveMode {
			cp, err := askForStartingOffset(topic, defaultCheckpoint)
			if err != nil {
				return nil, err
			}
			result[topic] = cp
		}
	}

	if !confirmConsumerStart(result, nil) {
		return nil, errExitInteractiveMode
	}

	return result, nil
}

func readUserData(consumer *kafka.Consumer,
	loader protobuf.Loader,
	topicFilter, typeFilter *regexp.Regexp,
	offsetInteractiveMode bool,
	defaultCheckpoint *kafka.PartitionCheckpoints) (map[string]*kafka.PartitionCheckpoints, map[string]string, error) {

	remoteTopics, err := consumer.GetTopics(topicFilter)
	if err != nil {
		return nil, nil, err
	}
	types, err := loader.List(typeFilter)
	if err != nil {
		return nil, nil, err
	}

	sort.Strings(remoteTopics)

	tm := make(map[string]string)
	topics := make(map[string]*kafka.PartitionCheckpoints, 0)

	topicIndexes, err := pickAnIndex("to consume from", "topic", remoteTopics, true)
	if err != nil {
		return nil, nil, err
	}

	sort.Strings(types)
	for _, index := range topicIndexes {
		topic := remoteTopics[index]
		topics[topic] = defaultCheckpoint
		typeIndexes, err := pickAnIndex(fmt.Sprintf("stored in '%s' topic", topic), "message type", types, false)
		if err != nil {
			return nil, nil, err
		}
		tm[topic] = types[typeIndexes[0]]
		if offsetInteractiveMode {
			cp, err := askForStartingOffset(topic, defaultCheckpoint)
			if err != nil {
				return nil, nil, err
			}
			topics[topic] = cp
		}
	}

	if !confirmConsumerStart(topics, tm) {
		return nil, nil, errExitInteractiveMode
	}

	return topics, tm, nil
}

// pickAnIndex returns the index of one of the items within the list
func pickAnIndex(msgSuffix, entryName string, input []string, multiSelect bool) (results []int, err error) {
	var cancelled bool
	go func() {
		internal.WaitForCancellationSignal()
		cancelled = true
	}()
	defer func() {
		if cancelled {
			results = nil
			err = errExitInteractiveMode
		}
	}()

	if len(input) == 0 {
		return nil, fmt.Errorf("no %s has been found. You may need to tweak the %[1]s filter", entryName)
	}
	for i, t := range input {
		fmt.Printf("%2d: %v\n", i+1, t)
	}

	multiSelect = multiSelect && len(input) > 1
	scanner := bufio.NewScanner(os.Stdin)
	message := fmt.Sprintf("Enter the index of the %s %s (Q to quit): ", entryName, msgSuffix)
	if multiSelect {
		message = fmt.Sprintf("Enter a comma separated list of %s indices %s (Q to quit): ", entryName, msgSuffix)
	}

	for fmt.Print(message); scanner.Scan(); fmt.Print(message) {
		trimmed := strings.TrimSpace(scanner.Text())
		if len(trimmed) == 0 {
			prefix := "O"
			if multiSelect {
				prefix = "At least o"
			}
			fmt.Printf("%sne %s must be selected.\n", prefix, entryName)
			continue
		}

		if askedToExit(trimmed) {
			return nil, errExitInteractiveMode
		}

		results = make([]int, 0)

		if !multiSelect {
			index := parseIndex(trimmed, entryName, len(input))
			if index < 0 {
				continue
			}
			results = append(results, index)
			return results, nil
		}

		parts := strings.Split(trimmed, ",")
		for _, part := range parts {
			index := parseIndex(part, entryName, len(input))
			if index < 0 {
				break
			}
			results = append(results, index)
		}
		if len(results) != len(parts) {
			continue
		}
		return results, nil
	}

	return nil, errExitInteractiveMode
}

func parseIndex(input, entryName string, length int) int {
	trimmed := strings.TrimSpace(input)
	i, err := strconv.Atoi(strings.TrimSpace(trimmed))
	if err != nil || i > length || i < 1 {
		fmt.Printf("The selected %s index should be between 1 and %d\n", entryName, len(input))
		return -1
	}
	return i - 1
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
		case "n", "no", "q", "quit", "exit":
			return false
		}
	}
	return false
}

func askForStartingOffset(topic string, defaultCP *kafka.PartitionCheckpoints) (cp *kafka.PartitionCheckpoints, err error) {
	var cancelled bool
	go func() {
		internal.WaitForCancellationSignal()
		cancelled = true
	}()
	defer func() {
		if cancelled {
			cp = nil
			err = errExitInteractiveMode
		}
	}()
	scanner := bufio.NewScanner(os.Stdin)
	msg := fmt.Sprintf("Enter the starting offset for %s topic. Press Enter to go with '%s' (Q to quit): ", topic, defaultCP.OriginalFromValue())
	for fmt.Print(msg); scanner.Scan(); fmt.Print(msg) {
		trimmed := strings.TrimSpace(scanner.Text())
		if len(trimmed) == 0 {
			return defaultCP, nil
		}
		if askedToExit(trimmed) {
			return nil, errExitInteractiveMode
		}
		cp, err := kafka.NewPartitionCheckpoints(trimmed)
		if err != nil {
			fmt.Printf("%s\n", internal.Title(err))
			continue
		}
		return cp, nil
	}
	return nil, errExitInteractiveMode
}

func confirmConsumerStart(topics map[string]*kafka.PartitionCheckpoints, contracts map[string]string) bool {
	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"Topic"}
	isProto := len(contracts) != 0
	if isProto {
		headers = append(headers, "Contract")
	}
	headers = append(headers, "Offset")
	table.SetHeader(headers)
	table.SetRowLine(true)
	for topic, cp := range topics {
		row := []string{topic}
		if isProto {
			row = append(row, contracts[topic])
		}
		row = append(row, cp.OriginalFromValue())
		table.Append(row)
	}
	fmt.Println()
	table.Render()
	return askForConfirmation("Start consuming")
}

func askedToExit(input string) bool {
	if strings.EqualFold(input, "Q") ||
		strings.EqualFold(input, "Quit") ||
		strings.EqualFold(input, "Exit") {
		return true
	}
	return false
}
