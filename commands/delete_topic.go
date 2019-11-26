package commands

import (
	"errors"
	"fmt"
	"regexp"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type deleteTopic struct {
	globalParams *GlobalParameters
	kafkaParams  *kafkaParameters
	topic        string
	interactive  bool
	topicFilter  *regexp.Regexp
	silent       bool
}

func addDeleteTopicSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &deleteTopic{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("delete", "Deletes a topic.").Action(cmd.run)
	c.Flag("topic", "The topic to remove.").
		Short('T').
		StringVar(&cmd.topic)
	c.Flag("interactive", "Runs the command in interactive mode. The --topic parameter will be ignored in this mode.").
		Short('i').
		BoolVar(&cmd.interactive)
	c.Flag("topic-filter", "An optional regular expression to filter the topics by (interactive mode only).").
		Short('t').
		RegexpVar(&cmd.topicFilter)
	c.Flag("silent", "Deletes the topic without user confirmation.").
		Short('s').
		BoolVar(&cmd.silent)
}

func (c *deleteTopic) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := initKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	if !c.interactive {
		return c.delete(manager, c.topic)
	}
	topics, err := manager.GetTopics(ctx, c.topicFilter, false, "")
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		fmt.Println(getNotFoundMessage("topic", "topic", c.topicFilter))
		return nil
	}

	names := topics.SortedTopics()
	index := pickAnIndex("Choose a topic to delete", "topic", names)
	if index < 0 {
		return nil
	}
	toRemove := names[index]
	return c.delete(manager, toRemove)
}

func (c *deleteTopic) delete(manager *kafka.Manager, topic string) error {
	if internal.IsEmpty(topic) {
		return errors.New("Topic cannot be empty.")
	}
	if c.silent || askForConfirmation(fmt.Sprintf("Are you sure you want to delete %s", topic)) {
		err := manager.DeleteTopic(topic)
		if err != nil {
			return err
		}
		fmt.Printf("%s topic has been deleted successfully.\n", topic)
	}
	return nil
}
