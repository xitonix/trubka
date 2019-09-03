package commands

import (
	"bytes"
	"context"
	"io"
	"io/ioutil"
	"os"
	"os/signal"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/gookit/color"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
	"github.com/xitonix/trubka/protobuf"
)

type consume struct {
	globalParams *GlobalParameters
	kafkaParams  *kafkaParameters

	protoRoot               string
	topic                   string
	messageType             string
	format                  string
	outputDir               string
	environment             string
	logFile                 string
	theme                   string
	topicFilter             *regexp.Regexp
	protoFilter             *regexp.Regexp
	searchQuery             *regexp.Regexp
	interactive             bool
	rewind                  bool
	reverse                 bool
	includeTimestamp        bool
	enableAutoTopicCreation bool
	timeCheckpoint          time.Time
	offsetCheckpoint        int64
}

// AddConsumeCommand initialises the consume command and adds it to the application.
func AddConsumeCommand(app *kingpin.Application, global *GlobalParameters) {
	cmd := &consume{
		globalParams: global,
	}
	c := app.Command("consume", "Starts consuming from the given Kafka topic.").Action(cmd.run)
	cmd.kafkaParams = bindKafkaFlags(c)
	cmd.bindCommandFlags(c)
}

func (c *consume) bindCommandFlags(command *kingpin.CmdClause) {
	command.Arg("topic", "The Kafka topic to consume from.").StringVar(&c.topic)
	command.Arg("proto", "The fully qualified name of the protocol buffers type, stored in the given topic.").
		StringVar(&c.messageType)
	command.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		ExistingDirVar(&c.protoRoot)
	command.Flag("rewind", "Starts consuming from the beginning of the stream.").Short('w').BoolVar(&c.rewind)
	var timeCheckpoint string
	command.Flag("from", "Starts consuming from the most recent available offset at the given time. This will override --rewind.").
		Short('f').
		Action(func(context *kingpin.ParseContext) error {
			t, err := parseTime(timeCheckpoint)
			if err != nil {
				return err
			}
			c.timeCheckpoint = t
			return nil
		}).
		StringVar(&timeCheckpoint)
	command.Flag("from-offset", `Starts consuming from the specified offset (if applicable). This will override --rewind and --from.
							If the most recent offset value of a partition is less than the specified value, this flag will be ignored.`).
		Default("-1").
		Short('o').
		Int64Var(&c.offsetCheckpoint)
	command.Flag("include-timestamp", "Prints the message timestamp before the content if it's been provided by Kafka.").
		Short('T').
		BoolVar(&c.includeTimestamp)
	command.Flag("auto-topic-creation", `Enables automatic Kafka topic creation before consuming (if it is allowed on the server). 
							Enabling this option in production is not recommended since it may pollute the environment with unwanted topics.`).
		BoolVar(&c.enableAutoTopicCreation)

	command.Flag("format", "The format in which the Kafka messages will be written to the output.").
		Default(protobuf.JsonIndent).
		EnumVar(&c.format,
			protobuf.Json,
			protobuf.JsonIndent,
			protobuf.Text,
			protobuf.TextIndent,
			protobuf.Hex,
			protobuf.HexIndent)
	command.Flag("output-dir", "The `directory` to write the Kafka messages to (Default: Stdout).").
		Short('d').
		StringVar(&c.outputDir)
	command.Flag("environment", `To store the offsets on the disk in environment specific paths. It's only required
									if you use Trubka to consume from different Kafka clusters on the same machine (eg. dev/prod).`).
		Short('e').
		Default("local").
		StringVar(&c.environment)

	command.Flag("reverse", "If set, the messages which match the --search-query will be filtered out.").
		BoolVar(&c.reverse)

	command.Flag("search-query", "The optional regular expression to filter the message content by.").
		Short('q').
		RegexpVar(&c.searchQuery)
	command.Flag("log-file", "The `file` to write the logs to. Set to 'none' to discard (Default: stdout).").
		Short('l').
		StringVar(&c.logFile)
	command.Flag("terminal-mode", `Sets the color mode of your terminal to adjust colors and highlights. Set to none to disable colors.`).
		Default(internal.DarkTheme).
		EnumVar(&c.theme, internal.NoTheme, internal.DarkTheme, internal.LightTheme)

	// Interactive mode flags
	command.Flag("interactive", "Runs the consumer in interactive mode.").
		Short('i').
		BoolVar(&c.interactive)

	command.Flag("topic-filter", "The optional regular expression to filter the remote topics by (Interactive mode only).").
		Short('t').
		RegexpVar(&c.topicFilter)

	command.Flag("proto-filter", "The optional regular expression to filter the proto types by (Interactive mode only).").
		Short('p').
		RegexpVar(&c.protoFilter)
}

func (c *consume) run(_ *kingpin.ParseContext) error {
	if !c.interactive {
		if internal.IsEmpty(c.topic) {
			return errors.New("Which Kafka topic you would like to consume from? Make sure you provide the topic as the first argument or switch to interactive mode (-i).")
		}
		if internal.IsEmpty(c.messageType) {
			return errors.Errorf("What message type is stored in %s topic? Make sure you provide the fully qualified proto name as the second argument or switch to interactive mode (-i).", c.topic)
		}
	}
	logFile, writeLogToFile, err := c.getLogWriter()
	if err != nil {
		return err
	}
	theme := internal.ColorTheme{}
	if !writeLogToFile && c.theme != internal.NoTheme {
		c.setLogColors(&theme)
	}

	prn := internal.NewPrinter(c.globalParams.Verbosity, logFile, theme)

	loader, err := protobuf.NewFileLoader(c.protoRoot)
	if err != nil {
		return err
	}

	saramaLogWriter := ioutil.Discard
	if c.globalParams.Verbosity >= internal.Chatty {
		saramaLogWriter = logFile
	}

	consumer, err := kafka.NewConsumer(
		c.kafkaParams.brokers, prn,
		c.environment,
		c.enableAutoTopicCreation,
		kafka.WithClusterVersion(c.kafkaParams.version),
		kafka.WithTLS(c.kafkaParams.tls),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(c.kafkaParams.saslMechanism,
			c.kafkaParams.saslUsername,
			c.kafkaParams.saslPassword))

	if err != nil {
		return err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		prn.Info(internal.Verbose, "Stopping Trubka.")
		cancel()
	}()

	topics := make(map[string]*kafka.Checkpoint)
	tm := make(map[string]string)
	cp := c.getCheckpoint()
	if c.interactive {
		topics, tm, err = readUserData(consumer, loader, c.topicFilter, c.protoFilter, cp)
		if err != nil {
			return err
		}
	} else {
		tm[c.topic] = c.messageType
		topics = getTopics(tm, cp)
	}

	for _, messageType := range tm {
		err := loader.Load(messageType)
		if err != nil {
			return err
		}
	}

	writers, writeEventsToFile, err := c.getOutputWriters(topics)
	if err != nil {
		return err
	}

	prn.Start(writers)

	wg := sync.WaitGroup{}

	if len(tm) > 0 {
		wg.Add(1)
		consumerCtx, stopConsumer := context.WithCancel(context.Background())
		defer stopConsumer()
		go func() {
			defer wg.Done()
			marshaller := protobuf.NewMarshaller(c.format, c.includeTimestamp)
			var searchColor color.Style
			if !writeEventsToFile {
				searchColor = c.getSearchColor()
			}
			var cancelled bool
			for {
				select {
				case <-ctx.Done():
					if !cancelled {
						stopConsumer()
						cancelled = true
					}
				case event, more := <-consumer.Events():
					if !more {
						return
					}
					if cancelled {
						// We keep consuming and let the Events channel to drain
						// Otherwise the consumer will deadlock
						continue
					}
					output, err := c.process(tm[event.Topic], loader, event, marshaller, searchColor)
					if err == nil {
						prn.WriteEvent(event.Topic, output)
						consumer.StoreOffset(event)
						continue
					}
					prn.Errorf(internal.Forced,
						"Failed to process the message at offset %d of partition %d, topic %s: %s",
						event.Offset,
						event.Partition,
						event.Topic,
						err)
				}
			}
		}()
		err = consumer.Start(consumerCtx, topics)
		if err != nil {
			prn.Errorf(internal.Forced, "Failed to start the consumer: %s", err)
		}
	} else {
		prn.Warning(internal.Forced, "Nothing to process. Terminating Trubka.")
	}

	// We still need to explicitly close the underlying Kafka client, in case `consumer.Start` has not been called.
	// It is safe to close the consumer twice.
	consumer.Close()
	wg.Wait()

	if err != nil {
		return err
	}

	// Do not write to Printer after this point
	if writeLogToFile {
		closeFile(logFile.(*os.File))
	}

	if writeEventsToFile {
		for _, w := range writers {
			closeFile(w.(*os.File))
		}
	}
	prn.Close()

	return nil
}

func (c *consume) getOutputWriters(topics map[string]*kafka.Checkpoint) (map[string]io.Writer, bool, error) {
	result := make(map[string]io.Writer)

	if internal.IsEmpty(c.outputDir) {
		for topic := range topics {
			result[topic] = os.Stdout
		}
		return result, false, nil
	}

	err := os.MkdirAll(c.outputDir, 0755)
	if err != nil {
		return nil, false, errors.Wrap(err, "Failed to create the output directory")
	}

	for topic := range topics {
		file := filepath.Join(c.outputDir, topic)
		lf, err := os.OpenFile(file, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, errors.Wrapf(err, "Failed to create: %s", file)
		}
		result[topic] = lf
	}

	return result, true, nil
}

func (c *consume) process(messageType string,
	loader *protobuf.FileLoader,
	event *kafka.Event,
	marshaller *protobuf.Marshaller,
	highlightColor color.Style) ([]byte, error) {

	msg, err := loader.Get(messageType)
	if err != nil {
		return nil, err
	}

	err = msg.Unmarshal(event.Value)
	if err != nil {
		return nil, err
	}

	output, err := marshaller.Marshal(msg, event.Timestamp)
	if err != nil {
		return nil, err
	}

	if c.searchQuery != nil {
		matches := c.searchQuery.FindAll(output, -1)
		if (matches != nil) == c.reverse {
			return nil, nil
		}
		for _, match := range matches {
			if highlightColor != nil {
				output = bytes.ReplaceAll(output, match, []byte(highlightColor.Sprint(string(match))))
			}
		}
	}

	return output, nil
}

func getTopics(topicMap map[string]string, cp *kafka.Checkpoint) map[string]*kafka.Checkpoint {
	topics := make(map[string]*kafka.Checkpoint)
	for topic := range topicMap {
		topics[topic] = cp
	}
	return topics
}

func (c *consume) getCheckpoint() *kafka.Checkpoint {
	cp := kafka.NewCheckpoint(c.rewind)
	switch {
	case c.offsetCheckpoint != -1:
		cp.SetOffset(c.offsetCheckpoint)
	case !c.timeCheckpoint.IsZero():
		cp.SetTimeOffset(c.timeCheckpoint)
	}
	return cp
}

func (c *consume) getLogWriter() (io.Writer, bool, error) {
	switch strings.TrimSpace(strings.ToLower(c.logFile)) {
	case "none":
		return ioutil.Discard, false, nil
	case "":
		return os.Stdout, false, nil
	default:
		lf, err := os.OpenFile(c.logFile, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0755)
		if err != nil {
			return nil, false, errors.Wrapf(err, "Failed to create: %s", c.logFile)
		}
		return lf, true, nil
	}
}

func (c *consume) setLogColors(theme *internal.ColorTheme) {
	switch c.theme {
	case internal.DarkTheme:
		theme.Error = color.New(color.LightRed)
		theme.Info = color.New(color.LightGreen)
		theme.Warning = color.New(color.LightYellow)
	case internal.LightTheme:
		theme.Error = color.New(color.FgRed)
		theme.Info = color.New(color.FgGreen)
		theme.Warning = color.New(color.FgYellow)
	}
}

func (c *consume) getSearchColor() color.Style {
	switch c.theme {
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

func closeFile(file *os.File) {
	err := file.Sync()
	if err != nil {
		color.Error.Printf("Failed to sync the file: %s\n", err)
	}
	if err := file.Close(); err != nil {
		color.Error.Printf("Failed to close the file: %s\n", err)
	}
}
