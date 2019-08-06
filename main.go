package main

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/jhump/protoreflect/dynamic"
	"github.com/pkg/errors"
	"github.com/pkg/profile"
	"github.com/xitonix/flags"
	"github.com/xitonix/flags/core"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/proto"
)

var version string

type marshaller func(msg *dynamic.Message, ts time.Time) ([]byte, error)

func main() {
	flags.EnableAutoKeyGeneration()
	flags.SetKeyPrefix("TBK")
	profilingMode := flags.String("profile", "Enables profiling.").WithValidRange(true, "cpu", "mem", "block", "mutex").Hide()

	brokers := flags.StringSlice("brokers", "The comma separated list of Kafka brokers in server:port format.").WithShort("b")
	protoDir := flags.String("proto-root", "The path to the folder where your *.proto files live.").WithShort("R")
	topic := flags.String("topic", `The Kafka topic to consume from.`).WithShort("t")
	messageType := flags.String("proto", `The fully qualified name of the protobuf type, stored in the given topic.`).WithShort("p")
	format := flags.String("format", "The format in which the Kafka messages will be written to the output.").
		WithValidRange(true, "json", "json-indent", "text", "text-indent", "hex", "hex-indent").
		WithDefault("json-indent")
	protoFiles := flags.StringSlice("proto-files", `An optional list of the proto files to load. If not specified all the files in --proto-root will be processed.`)

	interactive := flags.Bool("interactive", "Runs the tool in interactive mode.").WithShort("i")
	protoPrefix := flags.String("proto-prefix", "The optional prefix to prepend to proto message names.")
	topicPrefix := flags.String("topic-prefix", "The optional prefix to add to Kafka topic names.")

	logFilePath := flags.String("log-file", "The `file` to write the logs to. Set to '' to discard (Default: stdout).").WithShort("l")
	outputDir := flags.String("output-dir", "The `directory` to write the Kafka messages to. Set to '' to discard (Default: Stdout).").WithShort("d")

	kafkaVersion := flags.String("kafka-version", "Kafka cluster version.").WithDefault(kafka.DefaultClusterVersion)
	rewind := flags.Bool("rewind", `Starts consuming from the beginning of the stream.`).WithShort("w")
	timeCheckpoint := flags.Time("from", `Starts consuming from the most recent available offset at the given time. This will override --rewind.`).WithShort("f")
	offsetCheckpoint := flags.Int64("from-offset", `Starts consuming from the specified offset (if applicable). This will override --rewind and --from.
						If the most recent offset value of a partition is less than the specified value, this flag will be ignored.`).WithShort("o")
	environment := flags.String("environment", `To store the offsets on the disk in environment specific paths. It's only required
						if you use Trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).WithShort("E").WithDefault("offsets")
	topicFilter := flags.String("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").WithShort("n")
	typeFilter := flags.String("type-filter", "The optional regular expression to filter the proto types with (Interactive mode only).").WithShort("m")
	reverse := flags.Bool("reverse", "If set, the messages which match the --search-query will be filtered out.")
	searchQuery := flags.String("search-query", "The optional regular expression to filter the message content by.").WithShort("q")
	includeTimeStamp := flags.Bool("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").WithShort("T")
	enableAutoTopicCreation := flags.Bool("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
						Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`)
	saslMechanism := flags.String("sasl-mechanism", "SASL authentication mechanism.").
		WithValidRange(true, kafka.SASLMechanismNone, kafka.SASLMechanismPlain, kafka.SASLMechanismSCRAM256, kafka.SASLMechanismSCRAM512).
		WithDefault(kafka.SASLMechanismNone)

	saslUsername := flags.String("sasl-username", "SASL authentication username. Will be ignored if --sasl-mechanism is set to none.").WithShort("U")
	saslPassword := flags.String("sasl-password", "SASL authentication password. Will be ignored if --sasl-mechanism is set to none.").WithShort("P")

	enableTLS := flags.Bool("tls", "Enables TLS for communicating with the Kafka cluster.")
	certCA := flags.String("tls-ca", "An optional certificate authority file for TLS client authentication.")

	v := flags.Verbosity("The verbosity level of the tool.").WithKey("-")
	version := flags.Bool("version", "Prints the current version of Trubka.").WithKey("-")

	flags.Parse()

	if version.Get() {
		printVersion()
		return
	}

	if internal.IsEmpty(environment.Get()) {
		exit(errors.New("The environment cannot be empty."))
	}

	var searchExpression *regexp.Regexp
	if searchQuery.IsSet() {
		se, err := regexp.Compile(searchQuery.Get())
		if err != nil {
			exit(errors.Wrap(err, "Failed to parse the search query"))
		}
		searchExpression = se
	}

	if profilingMode.IsSet() {
		switch strings.ToLower(profilingMode.Get()) {
		case "cpu":
			defer profile.Start(profile.CPUProfile, profile.ProfilePath(".")).Stop()
		case "mem":
			defer profile.Start(profile.MemProfile, profile.ProfilePath(".")).Stop()
		case "mutex":
			defer profile.Start(profile.MutexProfile, profile.ProfilePath(".")).Stop()
		case "block":
			defer profile.Start(profile.BlockProfile, profile.ProfilePath(".")).Stop()
		}
	}

	logFile, closableLog, err := getLogWriter(logFilePath)
	if err != nil {
		exit(err)
	}

	prn := internal.NewPrinter(internal.ToVerbosityLevel(v.Get()), logFile)

	loader, err := proto.NewFileLoader(protoDir.Get(), protoPrefix.Get(), protoFiles.Get()...)
	if err != nil {
		exit(err)
	}

	var tlsConfig *tls.Config
	if enableTLS.Get() {
		tlsConfig, err = configureTLS(certCA.Get())
		if err != nil {
			exit(err)
		}
	}

	consumer, err := kafka.NewConsumer(
		brokers.Get(), prn,
		environment.Get(),
		enableAutoTopicCreation.Get(),
		kafka.WithClusterVersion(kafkaVersion.Get()),
		kafka.WithTLS(tlsConfig),
		kafka.WithSASL(saslMechanism.Get(), saslUsername.Get(), saslPassword.Get()))

	if err != nil {
		exit(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt)
		<-signals
		prn.Log(internal.Verbose, "Stopping Trubka.")
		cancel()
	}()

	topics := make(map[string]*kafka.Checkpoint)
	tm := make(map[string]string)
	cp := getCheckpoint(rewind.Get(), timeCheckpoint, offsetCheckpoint)
	if interactive.Get() {
		topics, tm, err = readUserData(consumer, loader, topicFilter.Get(), typeFilter.Get(), cp)
		if err != nil {
			exit(err)
		}
	} else {
		tm[topic.Get()] = messageType.Get()
		prefix := strings.TrimSpace(topicPrefix.Get())
		topics = getTopics(prefix, tm, cp)
	}

	writers, closable, err := getOutputWriters(outputDir, topics)
	if err != nil {
		exit(err)
	}

	prn.Start(writers)

	var marshal func(msg *dynamic.Message, ts time.Time) ([]byte, error)
	marshal = getMarshaller(format.Get(), includeTimeStamp.Get())

	if len(tm) > 0 {
		prn.Log(internal.Forced, "Consuming from:")
		for t, m := range tm {
			prn.Logf(internal.Forced, "    %s: %s", t, m)
		}
		reversed := reverse.Get()
		err = consumer.Start(ctx, topics, func(topic string, partition int32, offset int64, ts time.Time, key, value []byte) error {
			return consume(tm, topic, loader, value, ts, marshal, prn, searchExpression, reversed)
		})
	} else {
		prn.Log(internal.Forced, "Nothing to process. Terminating Trubka.")
	}

	// We still need to explicitly close the underlying Kafka client, in case `consumer.Start` has not been called.
	// It is safe to close the consumer twice.
	consumer.Close()

	prn.Close()

	if err != nil {
		exit(err)
	}

	if closableLog {
		closeFile(logFile.(*os.File))
	}

	if closable {
		for _, w := range writers {
			closeFile(w.(*os.File))
		}
	}
}

func configureTLS(caFilePath string) (*tls.Config, error) {
	caFilePath = strings.TrimSpace(caFilePath)
	if len(caFilePath) == 0 {
		return &tls.Config{
			InsecureSkipVerify: true,
		}, nil
	}
	caCert, err := ioutil.ReadFile(caFilePath)
	if err != nil {
		return nil, errors.Wrap(err, "Failed to load the certificate authority file")
	}
	caCertPool := x509.NewCertPool()
	caCertPool.AppendCertsFromPEM(caCert)
	return &tls.Config{
		RootCAs:            caCertPool,
		InsecureSkipVerify: false,
	}, nil
}

func getCheckpoint(rewind bool, timeCheckpoint *core.TimeFlag, offsetCheckpoint *core.Int64Flag) *kafka.Checkpoint {
	cp := kafka.NewCheckpoint(rewind)
	switch {
	case offsetCheckpoint.IsSet():
		cp.SetOffset(offsetCheckpoint.Get())
	case timeCheckpoint.IsSet():
		cp.SetTimeOffset(timeCheckpoint.Get())
	}
	return cp
}

func printVersion() {
	if version == "" {
		version = "[build from source]"
	}
	fmt.Printf("Trubka %s\n", version)
}

func consume(tm map[string]string,
	topic string,
	loader *proto.FileLoader,
	value []byte,
	ts time.Time,
	serialise marshaller,
	prn *internal.SyncPrinter,
	search *regexp.Regexp,
	reverse bool) error {
	messageType, ok := tm[topic]
	if !ok || internal.IsEmpty(messageType) {
		return errors.New("the message type cannot be empty")
	}
	msg, err := loader.Load(messageType)
	if err != nil {
		return err
	}
	err = msg.Unmarshal(value)
	if err != nil {
		return err
	}
	output, err := serialise(msg, ts)
	if err != nil {
		return err
	}
	if search != nil && search.Match(output) == reverse {
		return nil
	}
	prn.WriteMessage(topic, output)
	return nil
}

func getTopics(prefix string, topicMap map[string]string, cp *kafka.Checkpoint) map[string]*kafka.Checkpoint {
	topics := make(map[string]*kafka.Checkpoint)
	for topic := range topicMap {
		if len(prefix) > 0 && !strings.HasPrefix(topic, prefix) {
			topic = prefix + topic
		}
		topics[topic] = cp
	}
	return topics
}

func exit(err error) {
	fmt.Printf("FATAL: %s\n", err)
	os.Exit(1)
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

func closeFile(file *os.File) {
	err := file.Sync()
	if err != nil {
		fmt.Printf("Failed to sync the file: %s\n", err)
	}
	if err := file.Close(); err != nil {
		fmt.Printf("Failed to close the file: %s\n", err)
	}
}

func getMarshaller(format string, includeTimestamp bool) marshaller {
	format = strings.TrimSpace(strings.ToLower(format))
	switch format {
	case "hex", "hex-indent":
		return func(msg *dynamic.Message, ts time.Time) ([]byte, error) {
			output, err := msg.Marshal()
			if err != nil {
				return nil, err
			}
			fm := "%X"
			if format == "hex-indent" {
				fm = "% X"
			}
			m := []byte(fmt.Sprintf(fm, output))
			if includeTimestamp && !ts.IsZero() {
				return prependTimestamp(ts, m), nil
			}
			return m, nil
		}
	case "text":
		return func(msg *dynamic.Message, ts time.Time) ([]byte, error) {
			m, err := msg.MarshalText()
			if err != nil {
				return nil, err
			}
			if includeTimestamp && !ts.IsZero() {
				return prependTimestamp(ts, m), nil
			}
			return m, nil
		}
	case "text-indent":
		return func(msg *dynamic.Message, ts time.Time) ([]byte, error) {
			m, err := msg.MarshalTextIndent()
			if err != nil {
				return nil, err
			}
			if includeTimestamp && !ts.IsZero() {
				return prependTimestamp(ts, m), nil
			}
			return m, nil
		}
	case "json":
		return func(msg *dynamic.Message, ts time.Time) ([]byte, error) {
			m, err := msg.MarshalJSON()
			if err != nil {
				return nil, err
			}
			if includeTimestamp && !ts.IsZero() {
				return prependTimestamp(ts, m), nil
			}
			return m, nil
		}
	default:
		return func(msg *dynamic.Message, ts time.Time) ([]byte, error) {
			m, err := msg.MarshalJSONIndent()
			if err != nil {
				return nil, err
			}
			if includeTimestamp && !ts.IsZero() {
				return prependTimestamp(ts, m), nil
			}
			return m, nil
		}
	}
}

func prependTimestamp(ts time.Time, in []byte) []byte {
	return append([]byte(fmt.Sprintf("[%s]\n", internal.FormatTimeUTC(ts))), in...)
}
