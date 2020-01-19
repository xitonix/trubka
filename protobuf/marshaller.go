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
	includeTopicName bool
	includeKey       bool
}

func NewMarshaller(format string, includeTimeStamp, includeTopicName, includeKey bool) *Marshaller {
	return &Marshaller{
		format:           strings.TrimSpace(strings.ToLower(format)),
		includeTimeStamp: includeTimeStamp,
		includeTopicName: includeTopicName,
		includeKey:       includeKey,
	}
}

func (m *Marshaller) Marshal(msg *dynamic.Message, key []byte, ts time.Time, topic string, partition int32) ([]byte, error) {
	var (
		result []byte
		err    error
	)

	switch m.format {
	case internal.Hex:
		result, err = m.marshalHex(msg, false)
	case internal.HexIndent:
		result, err = m.marshalHex(msg, true)
	case internal.Text:
		result, err = m.marshal(msg.MarshalText)
	case internal.TextIndent:
		result, err = m.marshal(msg.MarshalTextIndent)
	case internal.Json:
		result, err = m.marshal(msg.MarshalJSON)
	default:
		result, err = m.marshal(msg.MarshalJSONIndent)
	}

	if err != nil {
		return nil, err
	}

	if m.includeTimeStamp && !ts.IsZero() {
		result = internal.PrependTimestamp(ts, result)
	}
	if m.includeKey {
		result = internal.PrependKey(key, partition, result)
	}

	if m.includeTopicName {
		result = internal.PrependTopic(topic, result)
	}

	return result, nil
}

func (m *Marshaller) marshalHex(msg *dynamic.Message, indent bool) ([]byte, error) {
	output, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	fm := "%X"
	if indent {
		fm = "% X"
	}
	out := []byte(fmt.Sprintf(fm, output))
	return out, nil
}

func (m *Marshaller) marshal(fn func() ([]byte, error)) ([]byte, error) {
	out, err := fn()
	if err != nil {
		return nil, err
	}
	return out, nil
}
