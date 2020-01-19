package internal

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	Json       = "json"
	JsonIndent = "json-indent"
	Text       = "text"
	TextIndent = "text-indent"
	Hex        = "hex"
	HexIndent  = "hex-indent"
)

type Marshaller struct {
	format           string
	includeTimeStamp bool
	includeTopicName bool
	includeKey       bool
	enableColor      bool
}

func NewPlainTextMarshaller(format string, includeTimeStamp, includeTopicName, includeKey, enableColor bool) *Marshaller {
	return &Marshaller{
		format:           strings.TrimSpace(strings.ToLower(format)),
		includeTimeStamp: includeTimeStamp,
		includeTopicName: includeTopicName,
		includeKey:       includeKey,
		enableColor:      enableColor,
	}
}

func (m *Marshaller) Marshal(msg, key []byte, ts time.Time, topic string, partition int32) ([]byte, error) {
	var (
		result []byte
		err    error
	)
	switch m.format {
	case Hex:
		result, err = m.marshalHex(msg, false)
	case HexIndent:
		result, err = m.marshalHex(msg, true)
	case JsonIndent:
		result, err = m.indentJson(msg)
	default:
		result = msg
	}
	if err != nil {
		return nil, err
	}
	if m.includeTimeStamp && !ts.IsZero() {
		result = PrependTimestamp(ts, m.enableColor, result)
	}
	if m.includeKey {
		result = PrependKey(key, partition, m.enableColor, result)
	}

	if m.includeTopicName {
		result = PrependTopic(topic, m.enableColor, result)
	}
	return result, nil
}

func (m *Marshaller) marshalHex(msg []byte, indent bool) ([]byte, error) {
	fm := "%X"
	if indent {
		fm = "% X"
	}
	out := []byte(fmt.Sprintf(fm, msg))

	return out, nil
}

func (m *Marshaller) indentJson(msg []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := json.Indent(&buf, msg, "", "   ")
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
