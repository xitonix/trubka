package main

import (
	"crypto/tls"
	"crypto/x509"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"

	"github.com/gookit/color"
	"github.com/pkg/errors"
	"github.com/xitonix/flags"
	"github.com/xitonix/flags/core"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

var (
	profilingMode           *core.StringFlag
	protoDir                *core.StringFlag
	logFilePath             *core.StringFlag
	outputDir               *core.StringFlag
	topic                   *core.StringFlag
	messageType             *core.StringFlag
	format                  *core.StringFlag
	kafkaVersion            *core.StringFlag
	environment             *core.StringFlag
	topicFilter             *core.StringFlag
	typeFilter              *core.StringFlag
	searchQuery             *core.StringFlag
	saslUsername            *core.StringFlag
	saslPassword            *core.StringFlag
	saslMechanism           *core.StringFlag
	tlsCACert               *core.StringFlag
	tlsClientKey            *core.StringFlag
	tlsClientCert           *core.StringFlag
	terminalMode            *core.StringFlag
	brokers                 *core.StringSliceFlag
	protoFiles              *core.StringSliceFlag
	interactive             *core.BoolFlag
	reverse                 *core.BoolFlag
	includeTimeStamp        *core.BoolFlag
	rewind                  *core.BoolFlag
	enableTLS               *core.BoolFlag
	enableAutoTopicCreation *core.BoolFlag
	versionRequest          *core.BoolFlag
	timeCheckpoint          *core.TimeFlag
	offsetCheckpoint        *core.Int64Flag
	verbosity               *core.CounterFlag
)

func initFlags() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	profilingMode = flags.String("profile", "Enables profiling.").WithValidRange(true, "cpu", "mem", "block", "mutex", "thread").Hide()

	brokers = flags.StringSlice("brokers", "The comma separated list of Kafka brokers in server:port format.").WithShort("b")
	protoDir = flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("R")
	topic = flags.String("topic", `The Kafka topic to consume from.`).WithShort("t")
	messageType = flags.String("proto", `The fully qualified name of the protobuf type, stored in the given topic.`).WithShort("p")
	format = flags.String("format", "The format in which the Kafka messages will be written to the output.").
		WithValidRange(true, protobuf.Json, protobuf.JsonIndent, protobuf.Text, protobuf.TextIndent, protobuf.Hex, protobuf.HexIndent).
		WithDefault(protobuf.JsonIndent)
	protoFiles = flags.StringSlice("proto-files", `An optional list of the proto files to load. If not specified all the files in --proto-root will be processed.`)

	interactive = flags.Bool("interactive", "Runs the tool in interactive mode.").WithShort("i")

	logFilePath = flags.String("log-file", "The `file` to write the logs to. Set to '' to discard (Default: stdout).").WithShort("l")
	outputDir = flags.String("output-dir", "The `directory` to write the Kafka messages to. Set to '' to discard (Default: Stdout).").WithShort("d")

	kafkaVersion = flags.String("kafka-version", "Kafka cluster version.").WithDefault(kafka.DefaultClusterVersion)
	rewind = flags.Bool("rewind", `Starts consuming from the beginning of the stream.`).WithShort("w")
	timeCheckpoint = flags.Time("from", `Starts consuming from the most recent available offset at the given time. This will override --rewind.`).WithShort("f")
	offsetCheckpoint = flags.Int64("from-offset", `Starts consuming from the specified offset (if applicable). This will override --rewind and --from.
						If the most recent offset value of a partition is less than the specified value, this flag will be ignored.`).WithShort("o")
	environment = flags.String("environment", `To store the offsets on the disk in environment specific paths. It's only required
						if you use Trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).WithShort("E").WithDefault("offsets")
	topicFilter = flags.String("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").WithShort("n")
	typeFilter = flags.String("type-filter", "The optional regular expression to filter the proto types with (Interactive mode only).").WithShort("m")
	reverse = flags.Bool("reverse", "If set, the messages which match the --search-query will be filtered out.")
	searchQuery = flags.String("search-query", "The optional regular expression to filter the message content by.").WithShort("q")
	includeTimeStamp = flags.Bool("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").WithShort("T")
	enableAutoTopicCreation = flags.Bool("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
						Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`)
	saslMechanism = flags.String("sasl-mechanism", "SASL authentication mechanism.").
		WithValidRange(true, kafka.SASLMechanismNone, kafka.SASLMechanismPlain, kafka.SASLMechanismSCRAM256, kafka.SASLMechanismSCRAM512).
		WithDefault(kafka.SASLMechanismNone)

	saslUsername = flags.String("sasl-username", "SASL authentication username. Will be ignored if --sasl-mechanism is set to none.").WithShort("U")
	saslPassword = flags.String("sasl-password", "SASL authentication password. Will be ignored if --sasl-mechanism is set to none.").WithShort("P")

	enableTLS = flags.Bool("tls", "Enables TLS (Unverified by default). Mutual authentication can also be enabled by providing client key and certificate.")
	tlsCACert = flags.String("ca-cert", `Trusted root certificates for verifying the server. If not set, Trubka will skip server certificate and domain verification.`)
	tlsClientCert = flags.String("client-cert", `Client certification file to enable mutual TLS authentication. Client key must also be provided.`)
	tlsClientKey = flags.String("client-key", `Client private key file to enable mutual TLS authentication. Client certificate must also be provided.`)
	terminalMode = flags.String("terminal-mode", `Sets the color mode of your terminal to adjust colors and highlights. Set to none to disable colors.`).
		WithValidRange(true, internal.NoTheme, internal.DarkTheme, internal.LightTheme).
		WithDefault(internal.DarkTheme)
	verbosity = flags.Verbosity("The verbosity level of the tool.").WithKey("-")
	versionRequest = flags.Bool("version", "Prints the current version of Trubka.").WithKey("-")

	flags.Parse()
}

func getLogWriter(f *core.StringFlag) (io.Writer, bool, error) {
	file := f.Get()
	if internal.IsEmpty(file) {
		if f.IsSet() {
			return ioutil.Discard, false, nil
		}
		return os.Stdout, false, nil
	}
	lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
	if err != nil {
		return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
	}
	return lf, true, nil
}

func getOutputWriters(dir *core.StringFlag, topics map[string]*kafka.Checkpoint) (map[string]io.Writer, bool, error) {
	root := dir.Get()
	result := make(map[string]io.Writer)

	if internal.IsEmpty(root) {
		discard := dir.IsSet()
		for topic := range topics {
			if discard {
				result[topic] = ioutil.Discard
				continue
			}
			result[topic] = os.Stdout
		}
		return result, false, nil
	}

	err := os.MkdirAll(root, 0755)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to create the output directory")
	}

	for topic := range topics {
		file := filepath.Join(root, topic)
		lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
		}
		result[topic] = lf
	}

	return result, true, nil
}

func configureTLS() (*tls.Config, error) {
	var tlsConf tls.Config

	clientCert := tlsClientCert.Get()
	clientKey := tlsClientKey.Get()

	// Mutual authentication is enabled. Both client key and certificate are needed.
	if !internal.IsEmpty(clientCert) {
		if internal.IsEmpty(clientKey) {
			return nil, errors.New("TLS client key is missing. Mutual authentication cannot be used")
		}
		certificate, err := tls.LoadX509KeyPair(clientCert, clientKey)
		if err != nil {
			return nil, errors.Wrap(err, "Failed to load the client TLS key pair")
		}
		tlsConf.Certificates = []tls.Certificate{certificate}
	}

	caCert := tlsCACert.Get()

	if internal.IsEmpty(caCert) {
		// Server cert verification will be disabled.
		// Only standard trusted certificates are used to verify the server certs.
		tlsConf.InsecureSkipVerify = true
		return &tlsConf, nil
	}
	certPool := x509.NewCertPool()
	ca, err := ioutil.ReadFile(caCert)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to read the CA certificate")
	}

	if ok := certPool.AppendCertsFromPEM(ca); !ok {
		return nil, errors.New("failed to append the CA certificate to the pool")
	}

	tlsConf.RootCAs = certPool

	return &tlsConf, nil
}

func getSearchColor(mode string) color.Style {
	switch mode {
	case internal.NoTheme:
		return nil
	case internal.DarkTheme:
		return color.New(color.FgYellow, color.Bold)
	case internal.LightTheme:
		return color.New(color.FgBlue, color.Bold)
	default:
		return nil
	}
}

func getColorTheme(mode string, toFile bool) internal.ColorTheme {
	theme := internal.ColorTheme{}
	if toFile {
		return theme
	}
	switch mode {
	case internal.DarkTheme:
		theme.Error = color.New(color.LightRed)
		theme.Info = color.New(color.LightGreen)
		theme.Warning = color.New(color.LightYellow)
	case internal.LightTheme:
		theme.Error = color.New(color.FgRed)
		theme.Info = color.New(color.FgGreen)
		theme.Warning = color.New(color.FgYellow)
	}
	return theme
}
