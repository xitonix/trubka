package list

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
)

// AddCommands adds the list command to the app.
func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("list", "A command to list Kafka entities.")
	addTopicsSubCommand(parent, global, kafkaParams)
	addGroupsSubCommand(parent, global, kafkaParams)
	addGroupOffsetsSubCommand(parent, global, kafkaParams)
	addLocalOffsetsSubCommand(parent, global, kafkaParams)
	addLocalTopicsSubCommand(parent, global)
}
