package main

import (
	"crypto/tls"
	"crypto/x509"
	"io/ioutil"
	"os"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type tlsParams struct {
	enabled    bool
	caCert     string
	clientCert string
	clientKey  string
}

func newApplication() error {
	app := kingpin.New("trubka", "A tool to consume protocol buffer events from Kafka.").DefaultEnvars()
	params := &commands.Parameters{}

	bindAppFlags(app, params)
	bindKafkaFlags(app, params)
	bindSASLFlags(app, params)
	tlsParams := bindTLSFlags(app)

	app.PreAction(func(ctx *kingpin.ParseContext) error {
		if !tlsParams.enabled {
			return nil
		}
		tlsConfig, err := configureTLS(tlsParams)
		if err != nil {
			return err
		}
		params.TLS = tlsConfig
		return nil
	})

	commands.AddVersionCommand(app, version)
	commands.AddConsumeCommand(app, params)
	commands.AddQueryCommand(app, params)
	commands.AddLocalOffsetCommand(app, params)
	_, err := app.Parse(os.Args[1:])
	return err
}

func bindAppFlags(app *kingpin.Application, params *commands.Parameters) {
	var verbosity int
	app.Flag("verbose", "The verbosity level of Trubka.").
		Short('v').
		NoEnvar().
		PreAction(func(context *kingpin.ParseContext) error {
			params.Verbosity = internal.ToVerbosityLevel(verbosity)
			return nil
		}).
		CounterVar(&verbosity)
}

func bindKafkaFlags(app *kingpin.Application, params *commands.Parameters) {
	app.Flag("brokers", "The comma separated list of Kafka brokers in server:port format.").
		Short('b').
		StringsVar(&params.Brokers)
	app.Flag("kafka-version", "Kafka cluster version.").
		Default(kafka.DefaultClusterVersion).
		StringVar(&params.KafkaVersion)
}

func bindTLSFlags(app *kingpin.Application) *tlsParams {
	t := &tlsParams{}
	app.Flag("tls", "Enables TLS (Unverified by default). Mutual authentication can also be enabled by providing client key and certificate.").
		BoolVar(&t.enabled)
	app.Flag("ca-cert", `Trusted root certificates for verifying the server. If not set, Trubka will skip server certificate and domain verification.`).
		ExistingFileVar(&t.caCert)
	app.Flag("client-cert", `Client certification file to enable mutual TLS authentication. Client key must also be provided.`).
		ExistingFileVar(&t.clientCert)
	app.Flag("client-key", `Client private key file to enable mutual TLS authentication. Client certificate must also be provided.`).
		ExistingFileVar(&t.clientKey)
	return t
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

func configureTLS(params *tlsParams) (*tls.Config, error) {
	tlsConf := tls.Config{}

	// Mutual authentication is enabled. Both client key and certificate are needed.
	if !internal.IsEmpty(params.clientCert) {
		if internal.IsEmpty(params.clientKey) {
			return nil, errors.New("TLS client key is missing. Mutual authentication cannot be used")
		}
		certificate, err := tls.LoadX509KeyPair(params.clientCert, params.clientKey)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to load the client TLS key pair")
		}
		tlsConf.Certificates = []tls.Certificate{certificate}
	}

	if internal.IsEmpty(params.caCert) {
		// Server cert verification will be disabled.
		// Only standard trusted certificates are used to verify the server certs.
		tlsConf.InsecureSkipVerify = true
		return &tlsConf, nil
	}
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(params.caCert)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read the CA certificate")
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append the CA certificate to the pool")
	}

	tlsConf.RootCAs = certPool

	return &tlsConf, nil
}
