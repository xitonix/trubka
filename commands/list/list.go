package list

import (
	"github.com/dustin/go-humanize"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("list", "A command to list Kafka entities.")
	addTopicsSubCommand(parent, global, kafkaParams)
	addGroupsSubCommand(parent, global, kafkaParams)
	addGroupOffsetsSubCommand(parent, global, kafkaParams)
	addLocalOffsetsSubCommand(parent, global, kafkaParams)
	addLocalTopicsSubCommand(parent, global)
}

func highlightLag(input int64, colorEnabled bool) interface{} {
	humanised := humanize.Comma(input)
	if !colorEnabled {
		return humanised
	}
	if input > 0 {
		return internal.Yellow(humanised, true)
	}
	return internal.Green(humanised, true)
}
