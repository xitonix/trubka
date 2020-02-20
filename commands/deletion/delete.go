package deletion

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
)

func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("delete", "A command to delete Kafka entities.")
	addDeleteTopicSubCommand(parent, global, kafkaParams)
	addDeleteGroupSubCommand(parent, global, kafkaParams)
	addDeleteLocalOffsetsSubCommand(parent, global)
}
