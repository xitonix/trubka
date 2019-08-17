package main

import (
	"github.com/xitonix/flags"
	"github.com/xitonix/flags/core"

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

	verbosity = flags.Verbosity("The verbosity level of the tool.").WithKey("-")
	versionRequest = flags.Bool("version", "Prints the current version of Trubka.").WithKey("-")

	flags.Parse()
}
