package protobuf

import (
	"encoding/base64"
	"fmt"
	"strings"
	"time"

	"github.com/golang/protobuf/jsonpb"
	"github.com/jhump/protoreflect/dynamic"

	"github.com/xitonix/trubka/internal"
)

type Marshaller struct {
	outputEncoding       string
	includeTimeStamp     bool
	includeTopicName     bool
	includeKey           bool
	enableColor          bool
	highlighter          *internal.JsonHighlighter
	indentedMarshaller   *jsonpb.Marshaler
	unIndentedMarshaller *jsonpb.Marshaler
}

func NewMarshaller(outputEncoding string, includeTimeStamp, includeTopicName, includeKey bool, enableColor bool, highlightStyle string) *Marshaller {
	return &Marshaller{
		outputEncoding:       strings.TrimSpace(strings.ToLower(outputEncoding)),
		includeTimeStamp:     includeTimeStamp,
		includeTopicName:     includeTopicName,
		includeKey:           includeKey,
		enableColor:          enableColor,
		highlighter:          internal.NewJsonHighlighter(highlightStyle, enableColor),
		indentedMarshaller:   newJsonMarshaller("  "),
		unIndentedMarshaller: newJsonMarshaller(""),
	}
}

func (m *Marshaller) Marshal(msg *dynamic.Message, key []byte, ts time.Time, topic string, partition int32) ([]byte, error) {
	var (
		result []byte
		err    error
	)

	switch m.outputEncoding {
	case internal.Base64Encoding:
		result, err = m.marshalBase64(msg)
	case internal.HexEncoding:
		result, err = m.marshalHex(msg)
	case internal.JsonEncoding:
		result, err = msg.MarshalJSONPB(m.unIndentedMarshaller)
	default:
		result, err = msg.MarshalJSONPB(m.indentedMarshaller)
		if m.enableColor {
			result = m.highlighter.Highlight(result)
		}
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

func (m *Marshaller) marshalBase64(msg *dynamic.Message) ([]byte, error) {
	output, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	buf := make([]byte, base64.StdEncoding.EncodedLen(len(output)))
	base64.StdEncoding.Encode(buf, output)
	return buf, nil
}

func newJsonMarshaller(indent string) *jsonpb.Marshaler {
	return &jsonpb.Marshaler{
		EnumsAsInts:  false,
		EmitDefaults: false,
		Indent:       indent,
		OrigName:     true,
		AnyResolver:  nil,
	}
}

func (m *Marshaller) marshalHex(msg *dynamic.Message) ([]byte, error) {
	output, err := msg.Marshal()
	if err != nil {
		return nil, err
	}
	out := []byte(fmt.Sprintf("%X", output))
	return out, nil
}
