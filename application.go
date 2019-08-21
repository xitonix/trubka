package main

import (
	"os"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

func newApplication() error {
	app := kingpin.New("trubka", "A tool to consume protocol buffer events from Kafka.").DefaultEnvars()
	params := &commands.Parameters{}

	bindAppFlags(app, params)

	bindKafkaFlags(app, params)
	bindSASLFlags(app, params)
	bindTLSFlags(app, params)

	commands.AddVersion(app, version)
	commands.AddConsume(app, params)
	_, err := app.Parse(os.Args[1:])
	return err
}

func bindAppFlags(app *kingpin.Application, params *commands.Parameters) {
	app.Flag("log-file", "The `file` to write the logs to. Set to '' to discard (Default: stdout).").
		Short('l').
		StringVar(&params.LogFile)
	app.Flag("terminal-mode", `Sets the color mode of your terminal to adjust colors and highlights. Set to none to disable colors.`).
		Default(internal.DarkTheme).
		EnumVar(&params.Theme, internal.NoTheme, internal.DarkTheme, internal.LightTheme)
	var verbosity int
	app.Flag("verbose", "The verbosity level of Trubka.").Short('v').CounterVar(&verbosity)
	app.Action(func(context *kingpin.ParseContext) error {
		params.Verbosity = internal.ToVerbosityLevel(verbosity)
		return nil
	})
}

func bindKafkaFlags(app *kingpin.Application, params *commands.Parameters) {
	app.Flag("brokers", "The comma separated list of Kafka brokers in server:port format.").
		Short('b').
		StringsVar(&params.Brokers)
	app.Flag("kafka-version", "Kafka cluster version.").Default(kafka.DefaultClusterVersion).
		StringVar(&params.KafkaVersion)
}

func bindTLSFlags(app *kingpin.Application, params *commands.Parameters) {
	app.Flag("tls", "Enables TLS (Unverified by default). Mutual authentication can also be enabled by providing client key and certificate.").
		BoolVar(&params.TLS)
	app.Flag("ca-cert", `Trusted root certificates for verifying the server. If not set, Trubka will skip server certificate and domain verification.`).
		ExistingFileVar(&params.CACert)
	app.Flag("client-cert", `Client certification file to enable mutual TLS authentication. Client key must also be provided.`).
		ExistingFileVar(&params.ClientCert)
	app.Flag("client-key", `Client private key file to enable mutual TLS authentication. Client certificate must also be provided.`).
		ExistingFileVar(&params.ClientKey)
}

func bindSASLFlags(app *kingpin.Application, params *commands.Parameters) {
	app.Flag("sasl-mechanism", "SASL authentication mechanism.").
		Default(kafka.SASLMechanismNone).
		EnumVar(&params.SASLMechanism,
			kafka.SASLMechanismNone,
			kafka.SASLMechanismPlain,
			kafka.SASLMechanismSCRAM256,
			kafka.SASLMechanismSCRAM512)
	app.Flag("sasl-username", "SASL authentication username. Will be ignored if --sasl-mechanism is set to none.").
		Short('U').
		StringVar(&params.SASLUsername)
	app.Flag("sasl-password", "SASL authentication password. Will be ignored if --sasl-mechanism is set to none.").
		Short('P').
		StringVar(&params.SASLPassword)
}
