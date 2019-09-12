package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

func newApplication() error {
	app := kingpin.New("trubka", "A tool to consume protocol buffer events from Kafka.").DefaultEnvars()

	global := &commands.GlobalParameters{}
	bindAppFlags(app, global)

	commands.AddVersionCommand(app, version)
	commands.AddConsumeCommand(app, global)
	commands.AddBrokerCommand(app, global)
	commands.AddTopicCommand(app, global)
	commands.AddGroupCommand(app, global)
	commands.AddLocalOffsetCommand(app, global)
	_, err := app.Parse(os.Args[1:])
	return err
}

func bindAppFlags(app *kingpin.Application, global *commands.GlobalParameters) {
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
