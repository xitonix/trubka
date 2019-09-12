package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddTopicCommand initialises the topic top level command and adds it to the application.
func AddTopicCommand(app *kingpin.Application, global *GlobalParameters) {
	parent := app.Command("topic", "A command to manage Kafka topics.")
	kafkaParams := bindKafkaFlags(parent)
	addListTopicsSubCommand(parent, global, kafkaParams)
	addDeleteTopicSubCommand(parent, global, kafkaParams)
}
