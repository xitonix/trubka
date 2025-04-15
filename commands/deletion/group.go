package deletion

import (
	"errors"
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

type group struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters
	group        string
	silent       bool
}

func addDeleteGroupSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &group{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("group", "Deletes a consumer group.").Action(cmd.run)
	c.Arg("group", "The consumer group to delete.").
		Required().
		StringVar(&cmd.group)

	c.Flag("silent", "Deletes the consumer group without user confirmation.").
		Short('s').
		NoEnvar().
		BoolVar(&cmd.silent)
}

func (c *group) run(_ *kingpin.ParseContext) error {
	manager, _, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	if internal.IsEmpty(c.group) {
		return errors.New("the consumer group name cannot be empty")
	}
	if c.silent || commands.AskForConfirmation(fmt.Sprintf("Are you sure you want to delete %s group", c.group)) {
		err := manager.DeleteConsumerGroup(c.group)
		if err != nil {
			return err
		}
		fmt.Printf("%s group has been deleted successfully.\n", c.group)
	}

	return nil
}
