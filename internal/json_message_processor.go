package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"time"
)

// JSONIndentation the indentation of JSON output.
const JSONIndentation = "  "

// JSONMessageProcessor prepares json output for printing.
type JSONMessageProcessor struct {
	outputEncoding string
	enableColor    bool
	highlighter    *JSONHighlighter
	indent         bool
	inclusions     *MessageMetadata
	compact        bool
}

// NewJSONMessageProcessor creates a new instance of JSON message processor.
func NewJSONMessageProcessor(
	outputFormat string,
	inclusions *MessageMetadata,
	enableColor bool,
	highlightStyle string) *JSONMessageProcessor {
	return &JSONMessageProcessor{
		outputEncoding: outputFormat,
		inclusions:     inclusions,
		enableColor:    enableColor,
		highlighter:    NewJSONHighlighter(highlightStyle, enableColor),
		indent:         outputFormat == JSONIndentEncoding,
		compact:        outputFormat == JSONCompactEncoding,
	}
}

// Process prepares json output for printing.
//
// The method injects the metadata into the json object if required.
func (j *JSONMessageProcessor) Process(message, key []byte, ts time.Time, topic string, partition int32, offset int64) ([]byte, error) {
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
		output.Timestamp = FormatTime(ts)
	}

	var err error

	if j.compact {
		message, err = MarshalJson(output, true)
	} else if j.indent {
		message, err = json.MarshalIndent(output, "", JSONIndentation)
	} else {
		message, err = json.Marshal(output)
	}
	if err != nil {
		return nil, err
	}
	return j.highlight(message), nil
}

func (j *JSONMessageProcessor) highlight(input []byte) []byte {
	if j.indent {
		return j.highlighter.Highlight(input)
	}
	return input
}

func MarshalJson(data interface{}, compact bool) ([]byte, error) {
	if compact {
		marshal, err := json.Marshal(data)
		if err != nil {
			return nil, err
		}
		dst := &bytes.Buffer{}
		err = json.Compact(dst, marshal)
		if err != nil {
			return nil, err
		}
		compBytes, err := io.ReadAll(dst)
		if err != nil {
			return nil, err
		}
		return compBytes, nil
	} else {
		return json.MarshalIndent(data, "", "  ")
	}
}
