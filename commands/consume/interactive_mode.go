package consume

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

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

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
		return nil, commands.ErrExitInteractiveMode
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
		typeIndexes, err := pickAnIndex(fmt.Sprintf("stored in %s topic", topic), "message type", types, false)
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
		return nil, nil, commands.ErrExitInteractiveMode
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
			err = commands.ErrExitInteractiveMode
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
		message = fmt.Sprintf("Enter a comma separated list of %s indices %s (-:All, m-n:Range, Q to quit): ", entryName, msgSuffix)
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
			return nil, commands.ErrExitInteractiveMode
		}

		results := make(map[int]interface{})

		if !multiSelect {
			indices, err := parseIndices(trimmed, entryName, len(input))
			if err != nil {
				printError(err)
				continue
			}
			if len(indices) > 1 {
				fmt.Printf("Only one %s can be selected.\n", entryName)
				continue
			}
			return []int{indices[0]}, nil
		}

		parts := strings.Split(trimmed, ",")
		var failed bool
		for _, part := range parts {
			indices, err := parseIndices(part, entryName, len(input))
			if err != nil {
				failed = true
				printError(err)
				break
			}
			for _, index := range indices {
				results[index] = nil
			}
		}
		if failed {
			continue
		}
		return toIndices(results), nil
	}

	return nil, commands.ErrExitInteractiveMode
}

func printError(err error) {
	fmt.Printf("%s.\n", internal.Title(err))
}

func toIndices(m map[int]interface{}) []int {
	result := make([]int, 0)
	for k := range m {
		result = append(result, k)
	}
	return result
}

func parseIndices(input, entryName string, length int) ([]int, error) {
	trimmed := strings.TrimSpace(input)
	if trimmed == "-" {
		return getRange(0, length-1), nil
	}
	ranges := strings.Split(trimmed, "-")
	if len(ranges) == 2 {
		from, err := parseIndex(ranges[0], entryName, length)
		if err != nil {
			return nil, err
		}
		to, err := parseIndex(ranges[1], entryName, length)
		if err != nil {
			return nil, err
		}
		if from == to {
			return []int{to}, nil
		}
		if to < from {
			return nil, fmt.Errorf("invalid index range")
		}
		return getRange(from, to), nil
	}

	index, err := parseIndex(trimmed, entryName, length)
	if err != nil {
		return nil, err
	}
	return []int{index}, nil
}

func getRange(from, to int) []int {
	result := make([]int, 0)
	for i := from; i <= to; i++ {
		result = append(result, i)
	}
	return result
}

func parseIndex(input, entryName string, length int) (int, error) {
	trimmed := strings.TrimSpace(input)
	i, err := strconv.Atoi(trimmed)
	if err != nil {
		return -1, errors.New("invalid index")
	}
	if i > length || i < 1 {
		return -1, fmt.Errorf("the selected %s index must be between 1 and %d", entryName, length)
	}
	return i - 1, nil
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
			err = commands.ErrExitInteractiveMode
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
			return nil, commands.ErrExitInteractiveMode
		}
		cp, err := kafka.NewPartitionCheckpoints(trimmed)
		if err != nil {
			fmt.Printf("%s\n", internal.Title(err))
			continue
		}
		return cp, nil
	}
	return nil, commands.ErrExitInteractiveMode
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
	return commands.AskForConfirmation("Start consuming")
}

func askedToExit(input string) bool {
	if strings.EqualFold(input, "Q") ||
		strings.EqualFold(input, "Quit") ||
		strings.EqualFold(input, "Exit") {
		return true
	}
	return false
}
