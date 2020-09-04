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

// Marshaller protocol buffers output serializer.
type Marshaller struct {
	outputFormat   string
	inclusions     *internal.MessageMetadata
	enableColor    bool
	jsonMarshaller *jsonpb.Marshaler
	jsonProcessor  *internal.JSONMessageProcessor
}

// NewMarshaller creates a new protocol buffer Marshaller.
func NewMarshaller(
	outputFormat string,
	inclusions *internal.MessageMetadata,
	enableColor bool,
	highlightStyle string) *Marshaller {
	outputFormat = strings.TrimSpace(strings.ToLower(outputFormat))
	m := &Marshaller{
		outputFormat: outputFormat,
		inclusions:   inclusions,
		enableColor:  enableColor,
		jsonProcessor: internal.NewJSONMessageProcessor(
			outputFormat,
			inclusions,
			enableColor,
			highlightStyle),
	}

	var indentation string
	if m.outputFormat == internal.JSONIndentEncoding {
		indentation = internal.JSONIndentation
	}
	m.jsonMarshaller = newJSONMarshaller(indentation)
	return m
}

// Marshal serialises the proto message into bytes.
func (m *Marshaller) Marshal(msg *dynamic.Message, key []byte, ts time.Time, topic string, partition int32, offset int64) ([]byte, error) {
	var (
		result []byte
		err    error
	)

	switch m.outputFormat {
	case internal.Base64Encoding:
		result, err = m.marshalBase64(msg)
	case internal.HexEncoding:
		result, err = m.marshalHex(msg)
	default:
		message, err := msg.MarshalJSONPB(m.jsonMarshaller)
		if err != nil {
			return nil, err
		}
		return m.jsonProcessor.Process(message, key, ts, topic, partition, offset)
	}

	if err != nil {
		return nil, err
	}

	result = m.inclusions.Render(key, result, ts, topic, partition, offset, m.outputFormat == internal.Base64Encoding)

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

func newJSONMarshaller(indent string) *jsonpb.Marshaler {
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
