package produce

import (
	"encoding/base64"
	"fmt"
	"regexp"
	"strconv"
	"strings"
	"time"

	"github.com/brianvoe/gofakeit/v4"
	"github.com/jhump/protoreflect/dynamic"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/protobuf"
)

type proto struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	message      string
	key          string
	topic        string
	proto        string
	count        uint32
	protoRoot    string
	random       bool
	protoMessage *dynamic.Message
	textEx       *regexp.Regexp
	emailEx      *regexp.Regexp
	numEx        *regexp.Regexp
	hashEx       *regexp.Regexp
	bytesEx      *regexp.Regexp
	int64Ex      *regexp.Regexp
}

func addProtoSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &proto{
		kafkaParams:  kafkaParams,
		globalParams: global,
		textEx:       regexp.MustCompile(`\?+`),
		emailEx:      regexp.MustCompile(`\w*\[Email]\w*`),
		numEx:        regexp.MustCompile(`"(?i)\w*D\[.*]\w*"`),
		bytesEx:      regexp.MustCompile(`(?i)\w*B64\[.*]\w*`),
		int64Ex:      regexp.MustCompile(`(?i)"\w*\[Int64]\w*"`),
		hashEx:       regexp.MustCompile(`#+`),
	}
	c := parent.Command("proto", "Publishes protobuf messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("proto", "The proto to publish to.").Required().StringVar(&cmd.proto)
	c.Arg("content", "The JSON representation of the message. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	c.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&cmd.protoRoot)
	c.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(&cmd.key)
	c.Flag("count", "The number of messages to publish.").
		Default("1").
		Short('c').
		Uint32Var(&cmd.count)
	c.Flag("generate-random-data", "Replaces the random generator place holder functions with the random value.").
		Short('g').
		BoolVar(&cmd.random)
}

func (c *proto) run(_ *kingpin.ParseContext) error {
	value, err := getValue(c.message)
	if err != nil {
		return err
	}
	loader, err := protobuf.NewFileLoader(c.protoRoot)
	if err != nil {
		return err
	}

	err = loader.Load(c.proto)
	if err != nil {
		return err
	}

	message, err := loader.Get(c.proto)
	if err != nil {
		return err
	}

	c.protoMessage = message

	return produce(c.kafkaParams, c.globalParams, c.topic, c.key, value, c.serializeProto, c.count)
}

func (c *proto) serializeProto(value string) ([]byte, error) {
	if c.random {
		value = c.replaceRandomGenerator(value)
	}
	err := c.protoMessage.UnmarshalJSON([]byte(value))
	if err != nil {
		return nil, err
	}

	return c.protoMessage.Marshal()
}

func (c *proto) replaceRandomGenerator(value string) string {
	gofakeit.Seed(time.Now().UnixNano())
	value = c.replaceTextGenerators(value)
	value = c.replaceNumberGenerators(value)
	value = c.replaceBytesGenerators(value)
	value = c.replaceInt64Generators(value)
	fmt.Println(value)
	return value
}

func (c *proto) replaceTextGenerators(value string) string {
	value = c.textEx.ReplaceAllStringFunc(value, func(match string) string {
		return gofakeit.Lexify(match)
	})
	value = c.emailEx.ReplaceAllStringFunc(value, func(match string) string {
		return gofakeit.Email()
	})
	return value
}

func (c *proto) replaceBytesGenerators(value string) string {
	value = c.bytesEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "B64[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "]"))
		return base64.StdEncoding.EncodeToString([]byte(match))
	})
	return value
}

func (c *proto) replaceInt64Generators(value string) string {
	value = c.int64Ex.ReplaceAllStringFunc(value, func(match string) string {
		return strconv.FormatInt(gofakeit.Int64(), 10)
	})
	return value
}

func (c *proto) replaceNumberGenerators(value string) string {
	value = c.numEx.ReplaceAllStringFunc(value, func(match string) string {
		match = strings.Replace(match, "D[", "", 1)
		match = strings.TrimSpace(strings.Trim(match, "\"]"))
		// The first zero is treated specially in gofakeit.Numerify!
		if len(match) > 0 && match[0] == '0' {
			match = strings.Replace(match, "0", "*^*", 1)
		}
		return strings.Replace(gofakeit.Numerify(match), "*^*", "0", 1)
	})

	value = c.hashEx.ReplaceAllStringFunc(value, func(match string) string {
		return gofakeit.Numerify(match)
	})
	return value
}
