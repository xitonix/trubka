package deletion

import (
	"errors"
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

type topic struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters
	topic        string
	silent       bool
}

func addDeleteTopicSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &topic{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("topic", "Deletes a topic.").Action(cmd.run)
	c.Arg("topic", "The topic to delete.").
		Required().
		StringVar(&cmd.topic)

	c.Flag("silent", "Deletes the topic without user confirmation.").
		Short('s').
		BoolVar(&cmd.silent)
}

func (c *topic) run(_ *kingpin.ParseContext) error {
	manager, _, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	if internal.IsEmpty(c.topic) {
		return errors.New("topic cannot be empty")
	}
	if c.silent || commands.AskForConfirmation(fmt.Sprintf("Are you sure you want to delete %s topic", c.topic)) {
		err := manager.DeleteTopic(c.topic)
		if err != nil {
			return err
		}
		fmt.Printf("%s topic has been deleted successfully.\n", c.topic)
	}

	return nil
}
