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

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

type checkpointMode int8

const (
	startCheckpoint checkpointMode = iota
	stopCheckpoint
)

var errExitInteractiveMode = errors.New("exit")

func selectTopics(consumer *kafka.Consumer, topicFilter *regexp.Regexp) ([]string, error) {
	remoteTopics, err := consumer.GetTopics(topicFilter)
	if err != nil {
		return nil, err
	}

	if len(remoteTopics) <= 1 {
		return remoteTopics, nil
	}

	sort.Strings(remoteTopics)
	indexes, err := pickAnIndex("to consume from", "topic", remoteTopics, true)
	if err != nil {
		return nil, err
	}
	result := make([]string, len(indexes))
	for i, index := range indexes {
		result[i] = remoteTopics[index]
	}
	return result, nil
}

func askUserForTopics(consumer *kafka.Consumer,
	topic string,
	topicFilter *regexp.Regexp,
	offsetInteractiveMode bool,
	defaultCheckpoints *kafka.PartitionCheckpoints,
	exclusive bool) (map[string]*kafka.PartitionCheckpoints, error) {

	var (
		topics []string
		err    error
	)

	// Topic is not provided by the user.
	// Let's load the topics from the server.
	if internal.IsEmpty(topic) {
		topics, err = selectTopics(consumer, topicFilter)
		if err != nil {
			return nil, err
		}
	} else {
		topics = []string{topic}
	}

	result := make(map[string]*kafka.PartitionCheckpoints)
	for _, topic := range topics {
		result[topic] = defaultCheckpoints
		if offsetInteractiveMode {
			cp, err := readCheckpoints(topic, defaultCheckpoints, exclusive)
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
	topic, messageType string,
	topicFilter, typeFilter *regexp.Regexp,
	offsetInteractiveMode bool,
	defaultCheckpoints *kafka.PartitionCheckpoints,
	exclusive bool) (map[string]*kafka.PartitionCheckpoints, map[string]string, error) {
	var (
		topics, types []string
		err           error
	)

	// Topic is not provided by the user.
	// Let's load the topics from the server.
	if internal.IsEmpty(topic) {
		topics, err = selectTopics(consumer, topicFilter)
		if err != nil {
			return nil, nil, err
		}
	} else {
		topics = []string{topic}
	}

	// Message type is not provided by the user.
	// Let's load it from the disk.
	if internal.IsEmpty(messageType) {
		types, err = loader.List(typeFilter)
		if err != nil {
			return nil, nil, err
		}
		sort.Strings(types)
	} else {
		types = []string{messageType}
	}

	tm := make(map[string]string)
	checkpoints := make(map[string]*kafka.PartitionCheckpoints, 0)
	for _, topic := range topics {
		checkpoints[topic] = defaultCheckpoints
		if len(types) > 1 {
			typeIndexes, err := pickAnIndex(fmt.Sprintf("stored in %s topic", topic), "message type", types, false)
			if err != nil {
				return nil, nil, err
			}
			tm[topic] = types[typeIndexes[0]]
		} else {
			tm[topic] = types[0]
		}
		if offsetInteractiveMode {
			cp, err := readCheckpoints(topic, defaultCheckpoints, exclusive)
			if err != nil {
				return nil, nil, err
			}
			checkpoints[topic] = cp
		}
	}

	if !confirmConsumerStart(checkpoints, tm) {
		return nil, nil, errExitInteractiveMode
	}

	return checkpoints, tm, nil
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
			return nil, errExitInteractiveMode
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

	return nil, errExitInteractiveMode
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

func readCheckpoints(topic string, defaultCP *kafka.PartitionCheckpoints, exclusive bool) (cp *kafka.PartitionCheckpoints, err error) {
	var (
		cancelled bool
	)

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

	for {
		from, err := readCheckpoint(topic, startCheckpoint, defaultCP.From())
		if err != nil {
			return nil, err
		}

		to, err := readCheckpoint(topic, stopCheckpoint, defaultCP.To())
		if err != nil {
			return nil, err
		}

		cp, err = kafka.NewPartitionCheckpoints(from, to, exclusive)
		if err != nil {
			fmt.Printf("%s\n", internal.Title(err))
			continue
		}
		return cp, err
	}
}

func readCheckpoint(topic string, mode checkpointMode, defaultValue string) ([]string, error) {
	var (
		modeName string
		fallback []string
	)

	if defaultValue != "" {
		fallback = strings.Split(defaultValue, ",")
	}

	if mode == startCheckpoint {
		modeName = "start"
	} else {
		modeName = "stop"
		if defaultValue == "" {
			defaultValue = "None"
		}
	}

	scanner := bufio.NewScanner(os.Stdin)
	msg := fmt.Sprintf("Enter a comma separated list of %s conditions for %s topic. Press Enter to go with '%s' (Q to quit): ", modeName, topic, defaultValue)
	for fmt.Print(msg); scanner.Scan(); fmt.Print(msg) {
		trimmed := strings.TrimSpace(scanner.Text())
		if len(trimmed) == 0 {
			return fallback, nil
		}
		if askedToExit(trimmed) {
			return nil, errExitInteractiveMode
		}
		if trimmed != "" {
			return strings.Split(trimmed, ","), nil
		}
	}
	return nil, errExitInteractiveMode
}

func confirmConsumerStart(topics map[string]*kafka.PartitionCheckpoints, contracts map[string]string) bool {
	headers := []*tabular.Column{tabular.C("Topic")}
	isProto := len(contracts) != 0
	if isProto {
		headers = append(headers, tabular.C("Contract"))
	}
	headers = append(headers, tabular.C("From").Align(tabular.AlignCenter), tabular.C("To").Align(tabular.AlignCenter))
	table := tabular.NewTable(false, headers...)
	for topic, cp := range topics {
		to := cp.To()
		if to == "" {
			to = "-"
		}
		if isProto {
			table.AddRow(topic, contracts[topic], cp.From(), to)
			continue
		}
		table.AddRow(topic, cp.From(), to)
	}
	output.NewLines(1)
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
