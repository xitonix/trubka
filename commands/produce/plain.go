package produce

import (
	"context"
	"fmt"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/commands/produce/template"
	"github.com/xitonix/trubka/internal"
)

type plain struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	message      string
	key          string
	topic        string
	count        uint64
	parser       *template.Parser
	random       bool
	sleep        time.Duration
}

func addPlainSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &plain{
		kafkaParams:  kafkaParams,
		globalParams: global,
		parser:       template.NewParser(),
	}
	c := parent.Command("plain", "Publishes plain text messages to Kafka. The content can be arbitrary text, json, base64 or hex encoded strings.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("content", "The message content. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	addProducerFlags(c, &cmd.sleep, &cmd.key, &cmd.random, &cmd.count)
}

func (c *plain) run(_ *kingpin.ParseContext) error {
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

	return produce(ctx, c.kafkaParams, c.globalParams, c.topic, c.key, value, c.serialize, c.count, c.sleep)
}

func (c *plain) serialize(value string) ([]byte, error) {
	if !c.random {
		return []byte(value), nil
	}
	value, err := c.parser.Parse(value)
	if err != nil {
		return nil, err
	}
	if c.globalParams.Verbosity >= internal.Verbose {
		fmt.Printf("%s\n", value)
	}
	return []byte(value), nil
}
