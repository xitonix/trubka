package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddLocalOffsetCommand initialises the top level local offset management command and adds it to the application.
func AddLocalOffsetCommand(app *kingpin.Application, global *GlobalParameters) {
	parent := app.Command("local", "Manages the locally stored offsets.")
	addListOffsetsSubCommand(parent, global)
	addDeleteLocalOffsetsSubCommand(parent, global)
}
