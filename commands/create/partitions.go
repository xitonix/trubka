package create

import (
	"errors"
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

type partitions struct {
	globalParams       *commands.GlobalParameters
	kafkaParams        *commands.KafkaParameters
	topic              string
	numberOfPartitions int32
}

func addCreatePartitionsSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &partitions{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("partitions", "Increases the number of partitions of the given topic. If the topic has a key, the partition logic or ordering of the messages will be affected.").Action(cmd.run)
	c.Arg("topic", "The topic name.").
		Required().
		StringVar(&cmd.topic)

	c.Flag("number-of-partitions", "Number of partitions.").
		Short('p').
		Required().
		NoEnvar().
		Int32Var(&cmd.numberOfPartitions)
}

func (c *partitions) run(_ *kingpin.ParseContext) error {
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

	err = manager.CreatePartitions(c.topic, c.numberOfPartitions)
	if err == nil {
		fmt.Printf("The partitions of %s have been readjusted successfully.", c.topic)
	}

	return err
}
