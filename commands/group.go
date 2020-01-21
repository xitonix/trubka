package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddGroupCommand initialises the group top level command and adds it to the application.
func AddGroupCommand(app *kingpin.Application, global *GlobalParameters, kafkaParams *KafkaParameters) {
	parent := app.Command("group", "A command to manage consumer groups.")
	addListGroupsSubCommand(parent, global, kafkaParams)
	addDeleteGroupSubCommand(parent, global, kafkaParams)
	addListGroupTopicsSubCommand(parent, global, kafkaParams)
}
