package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddConsumeCommand initialises the consume top level command and adds it to the application.
func AddConsumeCommand(app *kingpin.Application, global *GlobalParameters) {
	parent := app.Command("consume", "A command to consume events from Kafka.")
	kafkaParams := bindKafkaFlags(parent)
	addConsumeProtoCommand(parent, global, kafkaParams)
	addConsumePlainCommand(parent, global, kafkaParams)
}
