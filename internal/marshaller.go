package internal

import (
	"bytes"
	"encoding/base64"
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
	Base64     = "base64"
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

type Marshaller struct {
	format           string
	includeTimeStamp bool
	includeTopicName bool
	includeKey       bool
	enableColor      bool
	highlighter      *JsonHighlighter
}

func NewPlainTextMarshaller(format string,
	includeTimeStamp bool,
	includeTopicName bool,
	includeKey bool,
	enableColor bool,
	highlightStyle string) *Marshaller {
	return &Marshaller{
		format:           strings.TrimSpace(strings.ToLower(format)),
		includeTimeStamp: includeTimeStamp,
		includeTopicName: includeTopicName,
		includeKey:       includeKey,
		enableColor:      enableColor,
		highlighter:      NewJsonHighlighter(highlightStyle, enableColor),
	}
}

func (m *Marshaller) Marshal(msg, key []byte, ts time.Time, topic string, partition int32) ([]byte, error) {
	var (
		result []byte
		err    error
	)
	switch m.format {
	case Base64:
		result, err = m.marshalBase64(msg)
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

func (m *Marshaller) marshalHex(msg []byte, indent bool) ([]byte, error) {
	fm := "%X"
	if indent {
		fm = "% X"
	}
	out := []byte(fmt.Sprintf(fm, msg))

	return out, nil
}

func (m *Marshaller) marshalBase64(msg []byte) ([]byte, error) {
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(msg)))
	base64.StdEncoding.Encode(buf, msg)
	return buf, nil
}

func (m *Marshaller) indentJson(msg []byte) ([]byte, error) {
	var buf bytes.Buffer
	err := json.Indent(&buf, msg, "", "   ")
	if err != nil {
		return nil, err
	}
	if m.enableColor {
		return m.highlighter.Highlight(buf.Bytes()), nil
	}
	return buf.Bytes(), nil
}
