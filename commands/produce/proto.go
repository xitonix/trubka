package produce

import (
	"context"
	"encoding/base64"
	"encoding/hex"
	"fmt"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/commands/produce/template"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/protobuf"
)

type proto struct {
	kafkaParams    *commands.KafkaParameters
	globalParams   *commands.GlobalParameters
	message        string
	key            string
	topic          string
	proto          string
	count          uint64
	protoRoot      string
	random         bool
	protoMessage   *dynamic.Message
	highlightStyle string
	highlighter    *internal.JSONHighlighter
	decodeFrom     string
	parser         *template.Parser
	sleep          time.Duration
}

func addProtoSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {

	cmd := &proto{
		kafkaParams:  kafkaParams,
		globalParams: global,
		parser:       template.NewParser(),
	}
	c := parent.Command("proto", "Publishes protobuf messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("proto", "The proto to publish to.").Required().StringVar(&cmd.proto)
	c.Arg("content", "The JSON/Base64/Hex representation of the message. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	c.Flag("decode-from", "The encoding of the message content. The default value is no encoding (json).").
		Short('D').
		Default(internal.JSONEncoding).
		EnumVar(&cmd.decodeFrom, internal.JSONEncoding, internal.Base64Encoding, internal.HexEncoding)
	c.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&cmd.protoRoot)
	addProducerFlags(c, &cmd.sleep, &cmd.key, &cmd.random, &cmd.count)
	c.Flag("style", fmt.Sprintf("The highlighting style of the Json message content. Applicable to --content-type=%s only. Set to 'none' to disable.", internal.JSONEncoding)).
		Default(internal.DefaultHighlightStyle).
		EnumVar(&cmd.highlightStyle,
			internal.HighlightStyles...)
}

func (c *proto) run(_ *kingpin.ParseContext) error {
	value, err := getValue(c.message)
	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		internal.WaitForCancellationSignal()
		cancel()
	}()

	loader, err := protobuf.LoadFiles(ctx, c.globalParams.Verbosity, c.protoRoot)
	if err != nil {
		return err
	}

	err = loader.Load(ctx, c.proto)
	if err != nil {
		return err
	}

	message, err := loader.Get(c.proto)
	if err != nil {
		return err
	}

	c.protoMessage = message
	c.highlighter = internal.NewJSONHighlighter(c.highlightStyle, c.globalParams.EnableColor)

	return produce(ctx, c.kafkaParams, c.globalParams, c.topic, c.key, value, c.serializeProto, c.count, c.sleep)
}

func (c *proto) serializeProto(value string) (result []byte, err error) {
	var isJSON bool
	switch strings.ToLower(c.decodeFrom) {
	case internal.Base64Encoding:
		result, err = base64.StdEncoding.DecodeString(value)
	case internal.HexEncoding:
		value = strings.ReplaceAll(value, " ", "")
		result, err = hex.DecodeString(value)
	default:
		isJSON = true
		if c.random {
			value, err = c.parser.Parse(value)
			if err != nil {
				return nil, err
			}
		}

		err = c.protoMessage.UnmarshalJSON([]byte(value))
		if err != nil {
			if !c.random {
				return nil, fmt.Errorf("failed to parse the input as json. If the schema has been produced using -g flag, you must use the same flag (-g) to enable template parsing when publishing to Kafka: %w", err)
			}
			return nil, err
		}

		result, err = c.protoMessage.Marshal()
	}
	if err == nil {
		c.printContent(value, isJSON)
	}
	return
}

func (c *proto) printContent(value string, json bool) {
	if c.globalParams.Verbosity < internal.Verbose {
		return
	}
	if json {
		fmt.Printf("%s\n", c.highlighter.Highlight([]byte(value)))
	} else {
		fmt.Printf("%s\n", value)
	}
}
