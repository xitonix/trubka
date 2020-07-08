package create

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
)

// AddCommands adds the create command to the app.
func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("create", "A command to create Kafka entities.")
	addCreateTopicSubCommand(parent, global, kafkaParams)
	addCreatePartitionsSubCommand(parent, global, kafkaParams)
}
