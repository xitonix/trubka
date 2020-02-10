package produce

import (
	"fmt"
	"regexp"
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
}

func addProtoSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &proto{
		kafkaParams:  kafkaParams,
		globalParams: global,
		textEx:       regexp.MustCompile(`(?i)\[Text\]`),
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
	fmt.Println(value)
	return value
}

func (c *proto) replaceTextGenerators(value string) string {
	//
	return value
}
