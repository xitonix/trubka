package commands

import (
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type deleteLocalOffsets struct {
	globalParams *GlobalParameters
	topicsFilter *regexp.Regexp
	environment  string
}

func addDeleteLocalOffsetsSubCommand(parent *kingpin.CmdClause, params *GlobalParameters) {
	cmd := &deleteLocalOffsets{
		globalParams: params,
	}
	c := parent.Command("delete", "Deletes the local offsets from the given environment.").Action(cmd.run)
	c.Flag("topic", "An optional regular expression to filter the topics by.").Short('t').RegexpVar(&cmd.topicsFilter)
	c.Arg("environment", "The environment of which the local offsets will be deleted.").
		Required().
		StringVar(&cmd.environment)
}

func (c *deleteLocalOffsets) run(_ *kingpin.ParseContext) error {
	offsetManager := kafka.NewLocalOffsetManager(c.globalParams.Verbosity)
	files, err := offsetManager.GetOffsetFiles(c.environment, c.topicsFilter)
	if err != nil {
		return err
	}

	if len(files) == 0 {
		msg := fmt.Sprintf("There is no local offset stored for %s environment.", c.environment)
		fmt.Println(internal.Yellow(msg, c.globalParams.EnableColor))
		return nil
	}

	topics := make([]string, len(files)+1)
	for i := 0; i < len(files); i++ {
		file := filepath.Base(files[i])
		topics[i] = file[:strings.LastIndex(file, ".")]
	}

	topics[len(files)] = "All"

	indices, err := pickAnIndex("to delete the offsets", "topic", topics, false)
	if err != nil {
		return filterError(err)
	}

	index := indices[0]

	removeAll := index == (len(topics) - 1)
	var path, msg string
	if removeAll {
		path = filepath.Dir(files[0])
		msg = fmt.Sprintf("The local offsets of %d topic(s) will be deleted from %s environment. Are you sure?", len(files), c.environment)
	} else {
		path = files[index]
		msg = fmt.Sprintf("The local offsets of %s topic will be deleted. Are you sure?", topics[index])
	}
	return confirmAndDelete(msg, path, removeAll)
}

func confirmAndDelete(message, path string, all bool) error {
	if askForConfirmation(message) {
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
