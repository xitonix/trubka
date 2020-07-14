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
	outputEncoding string
	enableColor    bool
	highlighter    *JsonHighlighter
	indent         bool
	inclusions     *MessageMetadata
}

// NewJsonMessageProcessor creates a new instance of JSON message processor.
func NewJsonMessageProcessor(
	outputFormat string,
	inclusions *MessageMetadata,
	enableColor bool,
	highlightStyle string) *JsonMessageProcessor {
	return &JsonMessageProcessor{
		outputEncoding: outputFormat,
		inclusions:     inclusions,
		enableColor:    enableColor,
		highlighter:    NewJsonHighlighter(highlightStyle, enableColor),
		indent:         outputFormat == JsonIndentEncoding,
	}
}

// Process prepares json output for printing.
//
// The method injects the metadata into the json object if required.
func (j *JsonMessageProcessor) Process(message, key []byte, ts time.Time, topic string, partition int32, offset int64) ([]byte, error) {
	if !j.inclusions.IsRequested() {
		return j.highlight(message), nil
	}

	output := struct {
		Topic        string          `json:"topic,omitempty"`
		Timestamp    string          `json:"timestamp,omitempty"`
		Partition    *int32          `json:"partition,omitempty"`
		PartitionKey string          `json:"key,omitempty"`
		Offset       *int64          `json:"offset,omitempty"`
		Message      json.RawMessage `json:"message"`
	}{
		Message: message,
	}

	if j.inclusions.Topic {
		output.Topic = topic
	}

	if j.inclusions.Partition {
		output.Partition = &partition
	}

	if j.inclusions.Offset {
		output.Offset = &offset
	}

	if j.inclusions.Key {
		output.PartitionKey = fmt.Sprintf("%X", key)
	}

	if j.inclusions.Timestamp {
		output.Timestamp = FormatTimeForMachine(ts)
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
