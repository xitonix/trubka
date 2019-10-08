package commands

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type kafkaParameters struct {
	brokers       string
	version       string
	tls           *tls.Config
	saslMechanism string
	saslUsername  string
	saslPassword  string
}

type tlsParameters struct {
	enabled    bool
	caCert     string
	clientCert string
	clientKey  string
}

func (p *kafkaParameters) createConsumer(prn internal.Printer, env string, autoTopic bool, logW io.Writer) (*kafka.Consumer, error) {
	brokers := getBrokers(p.brokers)
	return kafka.NewConsumer(
		brokers, prn,
		env,
		autoTopic,
		kafka.WithClusterVersion(p.version),
		kafka.WithTLS(p.tls),
		kafka.WithLogWriter(logW),
		kafka.WithSASL(p.saslMechanism,
			p.saslUsername,
			p.saslPassword))
}

func bindKafkaFlags(cmd *kingpin.CmdClause) *kafkaParameters {
	params := &kafkaParameters{}
	cmd.Flag("brokers", "The comma separated list of Kafka brokers in server:port format.").
		Short('b').
		StringVar(&params.brokers)
	cmd.Flag("kafka-version", "Kafka cluster version.").
		Default(kafka.DefaultClusterVersion).
		StringVar(&params.version)

	bindSASLFlags(cmd, params)

	tlsParams := bindTLSFlags(cmd)
	cmd.PreAction(func(ctx *kingpin.ParseContext) error {
		if !tlsParams.enabled {
			return nil
		}
		tlsConfig, err := configureTLS(tlsParams)
		if err != nil {
			return err
		}
		params.tls = tlsConfig
		return nil
	})
	return params
}

func bindTLSFlags(cmd *kingpin.CmdClause) *tlsParameters {
	t := &tlsParameters{}
	cmd.Flag("tls", "Enables TLS (Unverified by default). Mutual authentication can also be enabled by providing client key and certificate.").
		BoolVar(&t.enabled)
	cmd.Flag("ca-cert", `Trusted root certificates for verifying the server. If not set, Trubka will skip server certificate and domain verification.`).
		ExistingFileVar(&t.caCert)
	cmd.Flag("client-cert", `Client certification file to enable mutual TLS authentication. Client key must also be provided.`).
		ExistingFileVar(&t.clientCert)
	cmd.Flag("client-key", `Client private key file to enable mutual TLS authentication. Client certificate must also be provided.`).
		ExistingFileVar(&t.clientKey)
	return t
}

func bindSASLFlags(cmd *kingpin.CmdClause, params *kafkaParameters) {
	cmd.Flag("sasl-mechanism", "SASL authentication mechanism.").
		Default(kafka.SASLMechanismNone).
		EnumVar(&params.saslMechanism,
			kafka.SASLMechanismNone,
			kafka.SASLMechanismPlain,
			kafka.SASLMechanismSCRAM256,
			kafka.SASLMechanismSCRAM512)
	cmd.Flag("sasl-username", "SASL authentication username. Will be ignored if --sasl-mechanism is set to none.").
		Short('U').
		StringVar(&params.saslUsername)
	cmd.Flag("sasl-password", "SASL authentication password. Will be ignored if --sasl-mechanism is set to none.").
		Short('P').
		StringVar(&params.saslPassword)
}

func configureTLS(params *tlsParameters) (*tls.Config, error) {
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
