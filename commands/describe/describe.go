package describe

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
)

// AddCommands adds the describe command to the app.
func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("describe", "A command to describe a Kafka entity.")
	addGroupSubCommand(parent, global, kafkaParams)
	addBrokerSubCommand(parent, global, kafkaParams)
	addTopicSubCommand(parent, global, kafkaParams)
	addClusterSubCommand(parent, global, kafkaParams)
}
