package protobuf

import (
	"fmt"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"

	"github.com/xitonix/trubka/internal"
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

func NewMarshaller(format string, includeTimeStamp bool) *Marshaller {
	return &Marshaller{
		format:           strings.TrimSpace(strings.ToLower(format)),
		includeTimeStamp: includeTimeStamp,
	}
}

func (m *Marshaller) Marshal(msg *dynamic.Message, ts time.Time) ([]byte, error) {
	switch m.format {
	case "hex":
		return m.marshalHex(msg, ts, false)
	case "hex-indent":
		return m.marshalHex(msg, ts, true)
	case "text":
		return m.marshal(msg.MarshalText, ts)
	case "text-indent":
		return m.marshal(msg.MarshalTextIndent, ts)
	case "json":
		return m.marshal(msg.MarshalJSON, ts)
	default:
		return m.marshal(msg.MarshalJSONIndent, ts)
	}
}

func (m *Marshaller) marshalHex(msg *dynamic.Message, ts time.Time, indent bool) ([]byte, error) {
	output, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	fm := "%X"
	if indent {
		fm = "% X"
	}
	out := []byte(fmt.Sprintf(fm, output))
	if m.includeTimeStamp && !ts.IsZero() {
		return prependTimestamp(ts, out), nil
	}
	return out, nil
}

func prependTimestamp(ts time.Time, in []byte) []byte {
	return append([]byte(fmt.Sprintf("[%s]\n", internal.FormatTimeUTC(ts))), in...)
}

func (m *Marshaller) marshal(fn func() ([]byte, error), ts time.Time) ([]byte, error) {
	out, err := fn()
	if err != nil {
		return nil, err
	}
	if m.includeTimeStamp && !ts.IsZero() {
		return prependTimestamp(ts, out), nil
	}
	return out, nil
}
