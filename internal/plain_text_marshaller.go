package internal

import (
	"bytes"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

const (
	JsonEncoding       = "json"
	JsonIndentEncoding = "json-indent"
)

const (
	PlainTextEncoding = "plain"
	Base64Encoding    = "base64"
	HexEncoding       = "hex"
)

var HighlightStyles = []string{
	"autumn",
	"dracula",
	"emacs",
	"friendly",
	"fruity",
	"github",
	"lovelace",
	"monokai",
	"monokailight",
	"native",
	"paraiso-dark",
	"paraiso-light",
	"pygments",
	"rrt",
	"solarized-dark",
	"solarized-light",
	"swapoff",
	"tango",
	"trac",
	"vim",
	"none",
}

const DefaultHighlightStyle = "fruity"

type PlainTextMarshaller struct {
	includeTimeStamp bool
	includeTopicName bool
	includeKey       bool
	enableColor      bool
	inputEncoding    string
	outputEncoding   string
	highlighter      *JsonHighlighter
}

func NewPlainTextMarshaller(
	inputEncoding string,
	outputEncoding string,
	includeTimeStamp bool,
	includeTopicName bool,
	includeKey bool,
	enableColor bool,
	highlightStyle string) *PlainTextMarshaller {
	return &PlainTextMarshaller{
		inputEncoding:    strings.TrimSpace(strings.ToLower(inputEncoding)),
		outputEncoding:   strings.TrimSpace(strings.ToLower(outputEncoding)),
		includeTimeStamp: includeTimeStamp,
		includeTopicName: includeTopicName,
		includeKey:       includeKey,
		enableColor:      enableColor,
		highlighter:      NewJsonHighlighter(highlightStyle, enableColor),
	}
}

func (m *PlainTextMarshaller) Marshal(msg, key []byte, ts time.Time, topic string, partition int32) ([]byte, error) {

	result, err, mustEncode := m.decode(msg)

	if err != nil {
		return nil, err
	}

	if mustEncode {
		result, err = m.encode(result)
		if err != nil {
			return nil, err
		}
	}

	if m.includeTimeStamp && !ts.IsZero() {
		result = PrependTimestamp(ts, result)
	}
	if m.includeKey {
		result = PrependKey(key, partition, result)
	}

	if m.includeTopicName {
		result = PrependTopic(topic, result)
	}
	return result, nil
}

func (m *PlainTextMarshaller) decode(msg []byte) ([]byte, error, bool) {
	switch m.inputEncoding {
	case HexEncoding:
		if m.outputEncoding == HexEncoding {
			return msg, nil, false
		}
		buf := make([]byte, hex.DecodedLen(len(msg)))
		_, err := hex.Decode(buf, msg)
		if err != nil {
			return nil, err, false
		}
		return buf, nil, true
	case Base64Encoding:
		if m.outputEncoding == Base64Encoding {
			return msg, nil, false
		}
		buf := make([]byte, base64.StdEncoding.DecodedLen(len(msg)))
		_, err := base64.StdEncoding.Decode(buf, msg)
		if err != nil {
			return nil, err, false
		}
		return buf, nil, true
	default:
		return msg, nil, true
	}
}

func (m *PlainTextMarshaller) encode(decoded []byte) ([]byte, error) {
	switch m.outputEncoding {
	case HexEncoding:
		return m.marshalHex(decoded)
	case Base64Encoding:
		return m.marshalBase64(decoded)
	case JsonIndentEncoding:
		result, err := m.indentJson(decoded)
		if err != nil {
			return nil, err
		}
		if m.enableColor {
			result = m.highlighter.Highlight(result)
		}
		return result, nil
	default:
		return decoded, nil
	}
}

func (m *PlainTextMarshaller) marshalHex(msg []byte) ([]byte, error) {
	out := []byte(fmt.Sprintf("%X", msg))
	return out, nil
}

func (m *PlainTextMarshaller) marshalBase64(msg []byte) ([]byte, error) {
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(msg)))
	base64.StdEncoding.Encode(buf, msg)
	return buf, nil
}

func (m *PlainTextMarshaller) indentJson(msg []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := json.Indent(&buf, msg, "", "   ")
	if err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}
