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
}

func NewPlainTextMarshaller(format string, includeTimeStamp bool) *Marshaller {
	return &Marshaller{
		format:           strings.TrimSpace(strings.ToLower(format)),
		includeTimeStamp: includeTimeStamp,
	}
}

func (m *Marshaller) Marshal(msg []byte, ts time.Time) ([]byte, error) {
	switch m.format {
	case Hex:
		return m.marshalHex(msg, ts, false)
	case HexIndent:
		return m.marshalHex(msg, ts, true)
	case Text, Json: // If no JSON indentation is required, it's just plain text
		return m.marshalText(msg, ts)
	case JsonIndent:
		return m.indentJson(msg, ts)
	default:
		return msg, nil
	}
}

func (m *Marshaller) marshalHex(msg []byte, ts time.Time, indent bool) ([]byte, error) {
	fm := "%X"
	if indent {
		fm = "% X"
	}
	out := []byte(fmt.Sprintf(fm, msg))
	if m.includeTimeStamp && !ts.IsZero() {
		return PrependTimestamp(ts, out), nil
	}
	return out, nil
}

func (m *Marshaller) marshalText(msg []byte, ts time.Time) ([]byte, error) {
	if m.includeTimeStamp && !ts.IsZero() {
		return PrependTimestamp(ts, msg), nil
	}
	return msg, nil
}

func (m *Marshaller) indentJson(msg []byte, ts time.Time) ([]byte, error) {
	var buf bytes.Buffer
	err := json.Indent(&buf, msg, "", "   ")
	if err != nil {
		return nil, err
	}
	msg = buf.Bytes()
	if m.includeTimeStamp && !ts.IsZero() {
		return PrependTimestamp(ts, msg), nil
	}
	return msg, nil
}
