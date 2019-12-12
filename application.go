package main

import (
	"os"
	"runtime"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

func newApplication() error {
	app := kingpin.New("trubka", "A tool to consume protocol buffer events from Kafka.").DefaultEnvars()

	global := &commands.GlobalParameters{}
	bindAppFlags(app, global)

	commands.AddVersionCommand(app, version, commit, built, runtimeVer)
	commands.AddConsumeCommand(app, global)
	commands.AddBrokerCommand(app, global)
	commands.AddTopicCommand(app, global)
	commands.AddGroupCommand(app, global)
	commands.AddLocalOffsetCommand(app, global)
	_, err := app.Parse(os.Args[1:])
	return err
}

func bindAppFlags(app *kingpin.Application, global *commands.GlobalParameters) {
	colorFlag := app.Flag("color", "Enables colors in the standard output. To disable, use --no-color (Disabled by default on Windows).")

	if runtime.GOOS == "windows" {
		colorFlag.Default("false")
	} else {
		colorFlag.Default("true")
	}

	colorFlag.BoolVar(&global.EnableColor)

	app.PreAction(func(context *kingpin.ParseContext) error {
		enabledColor = global.EnableColor
		return nil
	})

	var verbosity int
	app.Flag("verbose", "The verbosity level of Trubka.").
		Short('v').
		NoEnvar().
		PreAction(func(context *kingpin.ParseContext) error {
			global.Verbosity = internal.ToVerbosityLevel(verbosity)
			return nil
		}).
		CounterVar(&verbosity)
}
