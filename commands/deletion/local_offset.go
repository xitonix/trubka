package deletion

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type localOffsets struct {
	globalParams *commands.GlobalParameters
	topic        string
	environment  string
}

func addDeleteLocalOffsetsSubCommand(parent *kingpin.CmdClause, params *commands.GlobalParameters) {
	cmd := &localOffsets{
		globalParams: params,
	}
	c := parent.Command("local-offsets", "Deletes the local offsets from the given environment.").Action(cmd.run)
	c.Arg("environment", "The case-sensitive environment of which the local offsets will be deleted.").
		Required().
		StringVar(&cmd.environment)
	c.Arg("topic", "The case-sensitive topic name to delete the local offsets of. Set to ALL to delete all the topics within the specified environment.").StringVar(&cmd.topic)
}

func (c *localOffsets) run(_ *kingpin.ParseContext) error {
	offsetManager := kafka.NewLocalOffsetManager(c.globalParams.Verbosity)
	path, err := offsetManager.GetOffsetFileOrRoot(c.environment, c.topic)
	if err != nil {
		return err
	}

	topicMode := !internal.IsEmpty(c.topic) && !strings.EqualFold(c.topic, "all")

	var msg string
	if topicMode {
		msg = fmt.Sprintf("The local offsets of %s topic will be deleted from %s environment. Are you sure", c.topic, c.environment)
	} else {
		msg = fmt.Sprintf("The local offsets of all the topics will be deleted from %s environment. Are you sure", c.environment)
	}
	return confirmAndDelete(msg, path, !topicMode)
}

func confirmAndDelete(message, path string, all bool) error {
	if commands.AskForConfirmation(message) {
		var err error
		if all {
			err = os.RemoveAll(path)
		} else {
			err = os.Remove(path)
		}
		if err != nil {
			return err
		}
		fmt.Println("The local offsets have been removed.")
	}
	return nil
}
