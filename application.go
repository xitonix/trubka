package main

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"runtime"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/commands/list"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

func newApplication() error {
	app := kingpin.New("trubka", "A CLI tool for Kafka.").DefaultEnvars()
	global := &commands.GlobalParameters{}
	bindAppFlags(app, global)
	commands.AddVersionCommand(app, version, commit, built, runtimeVer)
	kafkaParams := bindKafkaFlags(app)
	list.AddCommand(app, global, kafkaParams)
	//commands.AddConsumeCommand(app, global)
	//commands.AddTopicCommand(app, global)
	//commands.AddGroupCommand(app, global)
	//commands.AddLocalOffsetCommand(app, global)
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

func bindKafkaFlags(app *kingpin.Application) *commands.KafkaParameters {
	params := &commands.KafkaParameters{}
	app.Flag("brokers", "The comma separated list of Kafka brokers in server:port format.").
		Short('b').
		StringVar(&params.Brokers)
	app.Flag("kafka-version", "Kafka cluster version.").
		Default(kafka.DefaultClusterVersion).
		StringVar(&params.Version)

	bindSASLFlags(app, params)

	tlsParams := bindTLSFlags(app)
	app.PreAction(func(ctx *kingpin.ParseContext) error {
		if !tlsParams.Enabled {
			return nil
		}
		tlsConfig, err := configureTLS(tlsParams)
		if err != nil {
			return err
		}
		params.TLS = tlsConfig
		return nil
	})
	return params
}

func bindTLSFlags(app *kingpin.Application) *commands.TLSParameters {
	t := &commands.TLSParameters{}
	app.Flag("tls", "Enables TLS (Unverified by default). Mutual authentication can also be enabled by providing client key and certificate.").
		BoolVar(&t.Enabled)
	app.Flag("ca-cert", `Trusted root certificates for verifying the server. If not set, Trubka will skip server certificate and domain verification.`).
		ExistingFileVar(&t.CACert)
	app.Flag("client-cert", `Client certification file to enable mutual TLS authentication. Client key must also be provided.`).
		ExistingFileVar(&t.ClientCert)
	app.Flag("client-key", `Client private key file to enable mutual TLS authentication. Client certificate must also be provided.`).
		ExistingFileVar(&t.ClientKey)
	return t
}

func bindSASLFlags(app *kingpin.Application, params *commands.KafkaParameters) {
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

func configureTLS(params *commands.TLSParameters) (*tls.Config, error) {
	tlsConf := tls.Config{}

	// Mutual authentication is enabled. Both client key and certificate are needed.
	if !internal.IsEmpty(params.ClientCert) {
		if internal.IsEmpty(params.ClientKey) {
			return nil, errors.New("TLS client key is missing. Mutual authentication cannot be used")
		}
		certificate, err := tls.LoadX509KeyPair(params.ClientCert, params.ClientKey)
		if err != nil {
			return nil, fmt.Errorf("failed to load the client TLS key pair: %w", err)
		}
		tlsConf.Certificates = []tls.Certificate{certificate}
	}

	if internal.IsEmpty(params.CACert) {
		// Server cert verification will be disabled.
		// Only standard trusted certificates are used to verify the server certs.
		tlsConf.InsecureSkipVerify = true
		return &tlsConf, nil
	}
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(params.CACert)
	if err != nil {
		return nil, fmt.Errorf("failed to read the CA certificate: %w", err)
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append the CA certificate to the pool")
	}

	tlsConf.RootCAs = certPool

	return &tlsConf, nil
}
