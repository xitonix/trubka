package commands

import (
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/protobuf"
)

func (c *consume) bindOutputFlags(command *kingpin.CmdClause) {
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
		Short('E').
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
}

func (c *consume) bindInteractiveModeFlags(command *kingpin.CmdClause) {
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

func (c *consume) bindKafkaFlags(command *kingpin.CmdClause) {
	command.Arg("topic", "The Kafka topic to consume from.").Required().StringVar(&c.topic)
	command.Arg("proto", "The fully qualified name of the protocol buffers type, stored in the given topic.").
		Required().
		StringVar(&c.messageType)
	command.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('R').
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
}
