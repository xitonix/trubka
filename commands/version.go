package commands

import (
	"fmt"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"
)

//TODO: Make the version command a flag
type version struct {
	version    string
	commit     string
	built      string
	runtimeVer string
	app        *kingpin.Application
}

func AddVersionCommand(app *kingpin.Application, appVersion, commit, built, runtimeVer string) {
	cmd := &version{
		version:    appVersion,
		commit:     commit,
		built:      built,
		runtimeVer: runtimeVer,
		app:        app,
	}
	app.Command("version", "Prints the current version of Trubka.").Action(cmd.run)
}

func (c *version) run(*kingpin.ParseContext) error {
	if c.version == "" {
		c.version = "[built from source]"
	}
	b := strings.Builder{}
	b.WriteString("\nTrubka - A CLI Tool for Kafka\n")
	b.WriteString(fmt.Sprintf("  Version: %s\n", c.version))
	b.WriteString(fmt.Sprintf("  Runtime: %s\n", c.runtimeVer))
	b.WriteString(fmt.Sprintf("    Built: %s\n", c.built))
	b.WriteString(fmt.Sprintf("   Commit: %s\n", c.commit))
	fmt.Println(b.String())
	return nil
}
