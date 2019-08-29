package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddQueryCommand initialises the query top level command and adds it to the application.
func AddQueryCommand(app *kingpin.Application, global *GlobalParameters) {
	parent := app.Command("query", "Queries the information about the specified Kafka entity from the server.")
	kafkaParams := bindKafkaFlags(parent)
	addBrokersSubCommand(parent, global, kafkaParams)
	addTopicsSubCommand(parent, global, kafkaParams)
}
