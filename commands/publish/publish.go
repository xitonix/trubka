package publish

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
)

func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("publish", "A command to publish messages to kafka")
	addPlainSubCommand(parent, global, kafkaParams)
}
