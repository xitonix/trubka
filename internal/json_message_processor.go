package internal

import (
	"encoding/json"
	"fmt"
	"time"
)

// JsonIndentation the indentation of JSON output.
const JsonIndentation = "  "

// JsonMessageProcessor prepares json output for printing.
type JsonMessageProcessor struct {
	outputEncoding   string
	includeTimeStamp bool
	includeTopicName bool
	includeKey       bool
	enableColor      bool
	highlighter      *JsonHighlighter
	indent           bool
}

// NewJsonMessageProcessor creates a new instance of JSON message processor.
func NewJsonMessageProcessor(outputFormat string,
	includeTimeStamp,
	includeTopicName,
	includeKey bool,
	enableColor bool,
	highlightStyle string) *JsonMessageProcessor {
	return &JsonMessageProcessor{
		outputEncoding:   outputFormat,
		includeTimeStamp: includeTimeStamp,
		includeTopicName: includeTopicName,
		includeKey:       includeKey,
		enableColor:      enableColor,
		highlighter:      NewJsonHighlighter(highlightStyle, enableColor),
		indent:           outputFormat == JsonIndentEncoding,
	}
}

// Process prepares json output for printing.
//
// The method injects the metadata into the json object if required.
func (j *JsonMessageProcessor) Process(message, key []byte, ts time.Time, topic string, partition int32) ([]byte, error) {
	includeMetadata := j.includeTopicName || j.includeKey || (j.includeTimeStamp && !ts.IsZero())
	if !includeMetadata {
		return j.highlight(message), nil
	}

	output := struct {
		Topic        string          `json:"topic,omitempty"`
		Timestamp    string          `json:"timestamp,omitempty"`
		Partition    *int32          `json:"partition,omitempty"`
		PartitionKey string          `json:"key,omitempty"`
		Message      json.RawMessage `json:"message"`
	}{
		Message: message,
	}

	if j.includeTopicName {
		output.Topic = topic
	}

	if j.includeKey {
		output.PartitionKey = fmt.Sprintf("%X", key)
		output.Partition = &partition
	}

	if j.includeTimeStamp && !ts.IsZero() {
		output.Timestamp = FormatTimeUTC(ts)
	}
	var err error
	if j.indent {
		message, err = json.MarshalIndent(output, "", JsonIndentation)
	} else {
		message, err = json.Marshal(output)
	}
	if err != nil {
		return nil, err
	}
	return j.highlight(message), nil
}

func (j *JsonMessageProcessor) highlight(input []byte) []byte {
	if j.indent {
		return j.highlighter.Highlight(input)
	}
	return input
}
