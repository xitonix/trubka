package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"
)

// AddLocalOffsetCommand initialises the top level local offset management command and adds it to the application.
func AddLocalOffsetCommand(app *kingpin.Application, params *Parameters) {
	parent := app.Command("local", "Manages the locally stored offsets.")
	addListOffsetsSubCommand(parent, params)
}
