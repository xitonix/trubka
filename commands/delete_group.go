package commands

import (
	"errors"
	"fmt"
	"regexp"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type deleteGroup struct {
	globalParams *GlobalParameters
	kafkaParams  *kafkaParameters
	group        string
	interactive  bool
	groupFilter  *regexp.Regexp
	silent       bool
}

func addDeleteGroupSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &deleteGroup{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("delete", "Deletes an empty consumer group.").Action(cmd.run)
	c.Flag("group", "The consumer group name to remove.").
		Short('G').
		StringVar(&cmd.group)
	c.Flag("interactive", "Runs the command in interactive mode. The --group parameter will be ignored in this mode.").
		Short('i').
		BoolVar(&cmd.interactive)
	c.Flag("group-filter", "An optional regular expression to filter the groups by (interactive mode only).").
		Short('g').
		RegexpVar(&cmd.groupFilter)
	c.Flag("silent", "Deletes the consumer group without user confirmation.").
		Short('s').
		BoolVar(&cmd.silent)
}

func (c *deleteGroup) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := initKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		cancel()
		manager.Close()
	}()

	if !c.interactive {
		return c.delete(manager, c.group)
	}
	groups, err := manager.GetConsumerGroups(ctx, false, nil, c.groupFilter, nil)
	if err != nil {
		return err
	}

	if len(groups) == 0 {
		fmt.Println(getNotFoundMessage("consumer group", "group", c.groupFilter))
		return nil
	}

	names := groups.Names()
	sort.Strings(names)
	index := pickAnIndex("Choose a consumer group ID to delete", "group", names)
	if index < 0 {
		return nil
	}
	toRemove := names[index]
	return c.delete(manager, toRemove)
}

func (c *deleteGroup) delete(manager *kafka.Manager, group string) error {
	if internal.IsEmpty(group) {
		return errors.New("Consumer group cannot be empty.")
	}
	if c.silent || askForConfirmation(fmt.Sprintf("Are you sure you want to delete %s", group)) {
		err := manager.DeleteConsumerGroup(group)
		if err != nil {
			return err
		}
		fmt.Printf("%s consumer group has been deleted successfully.\n", group)
	}
	return nil
}
