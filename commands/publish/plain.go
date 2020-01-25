package publish

import (
	"context"

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
	count        int
}

func addPlainSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &plain{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("plain", "Publishes plain text messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("content", "The message content.").StringVar(&cmd.message)
	c.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(&cmd.key)
	c.Flag("counter", "The number of messages to publish").
		Short('c').
		IntVar(&cmd.count)
}

func (c *plain) run(_ *kingpin.ParseContext) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		internal.WaitForCancellationSignal()
		cancel()
	}()

	_ = ctx

	return nil
}
