package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddQueryCommand initialises the query top level command and adds it to the application.
func AddQueryCommand(app *kingpin.Application, params *Parameters) {
	parent := app.Command("query", "Queries the information about the specified entity.")
	addBrokersSubCommand(parent, params)
	addTopicsSubCommand(parent, params)
}
