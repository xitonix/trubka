package protobuf

import (
	"fmt"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"

	"github.com/xitonix/trubka/internal"
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
	case internal.Hex:
		return m.marshalHex(msg, ts, false)
	case internal.HexIndent:
		return m.marshalHex(msg, ts, true)
	case internal.Text:
		return m.marshal(msg.MarshalText, ts)
	case internal.TextIndent:
		return m.marshal(msg.MarshalTextIndent, ts)
	case internal.Json:
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
		return internal.PrependTimestamp(ts, out), nil
	}
	return out, nil
}

func (m *Marshaller) marshal(fn func() ([]byte, error), ts time.Time) ([]byte, error) {
	out, err := fn()
	if err != nil {
		return nil, err
	}
	if m.includeTimeStamp && !ts.IsZero() {
		return internal.PrependTimestamp(ts, out), nil
	}
	return out, nil
}
