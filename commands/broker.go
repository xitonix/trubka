package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddBrokerCommand initialises the broker top level command and adds it to the application.
func AddBrokerCommand(app *kingpin.Application, global *GlobalParameters) {
	parent := app.Command("broker", "A command to manage Kafka brokers.")
	kafkaParams := bindKafkaFlags(parent)
	addListBrokersSubCommand(parent, global, kafkaParams)
}
