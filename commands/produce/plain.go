package produce

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

type plain struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	message      string
	key          string
	topic        string
	count        uint32
}

func addPlainSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &plain{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("plain", "Publishes json/plain text messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("content", "The message content. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	c.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(&cmd.key)
	c.Flag("count", "The number of messages to publish.").
		Default("1").
		Short('c').
		Uint32Var(&cmd.count)

}

func (c *plain) run(_ *kingpin.ParseContext) error {
	value, err := getValue(c.message)
	if err != nil {
		return err
	}

	return produce(c.kafkaParams, c.globalParams, c.topic, c.key, value,
		func(value string) ([]byte, error) {
			if c.globalParams.Verbosity >= internal.Verbose {
				fmt.Printf("%s\n", value)
			}
			return []byte(value), nil
		},
		c.count)
}
