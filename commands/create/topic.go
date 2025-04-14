package create

import (
	"errors"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

type topic struct {
	globalParams       *commands.GlobalParameters
	kafkaParams        *commands.KafkaParameters
	topic              string
	numberOfPartitions int32
	replicationFactor  int16
	retention          time.Duration
	validateOnly       bool
	logger             internal.Logger
}

func addCreateTopicSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &topic{
		globalParams: global,
		kafkaParams:  kafkaParams,
		logger:       *internal.NewLogger(1),
	}
	c := parent.Command("topic", "Creates a new topic.").Action(cmd.run)
	c.Arg("topic", "The topic name.").
		Required().
		StringVar(&cmd.topic)

	c.Flag("number-of-partitions", "Number of partitions.").
		Short('p').
		Required().
		NoEnvar().
		Int32Var(&cmd.numberOfPartitions)

	c.Flag("replication-factor", "Replication factor.").
		Short('r').
		Required().
		NoEnvar().
		Int16Var(&cmd.replicationFactor)

	c.Flag("validate-only", "Validates the request instead of creating the topic.").
		Short('A').
		NoEnvar().
		BoolVar(&cmd.validateOnly)

	c.Flag("retention", "Topic retention period. Examples 300ms, 150s, 1.5h or 2h45m.").
		NoEnvar().
		DurationVar(&cmd.retention)
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

	err = manager.CreateTopic(c.topic, c.numberOfPartitions, c.replicationFactor, c.validateOnly, c.retention)
	if err == nil {
		if c.validateOnly {
			c.logger.Log(1, "The server WILL ACCEPT the request.")
		} else {
			c.logger.Logf(1, "Topic %s has been created successfully.", c.topic)
		}
	}

	return err
}
