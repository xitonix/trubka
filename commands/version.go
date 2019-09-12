package commands

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"
)

type version struct {
	version string
	app     *kingpin.Application
}

func AddVersionCommand(app *kingpin.Application, appVersion string) {
	cmd := &version{
		version: appVersion,
		app:     app,
	}
	app.Command("version", "Prints the current version of Trubka.").Action(cmd.run)
}

func (c *version) run(ctx *kingpin.ParseContext) error {
	if c.version == "" {
		c.version = "[built from source]"
	}
	fmt.Printf("%s %s\n", c.app.Name, c.version)
	return nil
}
