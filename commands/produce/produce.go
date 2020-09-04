package produce

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/kafka"
)

type valueSerializer func(raw string) ([]byte, error)

// AddCommands adds the produce command to the app.
func AddCommands(app *kingpin.Application, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	parent := app.Command("produce", "A command to publish messages to kafka.")
	addPlainSubCommand(parent, global, kafkaParams)
	addProtoSubCommand(parent, global, kafkaParams)
	addSchemaSubCommand(parent, global)
}

func addProducerFlags(cmd *kingpin.CmdClause, sleep *time.Duration, key *string, random *bool, count *uint64) {
	cmd.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(key)
	cmd.Flag("generate-random-data", "Replaces the random generator place holder functions in the content (if any) with random values.").
		Short('g').
		BoolVar(random)
	cmd.Flag("count", "The number of messages to publish. Set to zero to produce indefinitely.").
		Default("1").
		Short('c').
		Uint64Var(count)
	cmd.Flag("sleep", "The amount of time to wait before publishing each message to Kafka. Examples 500ms, 1s, 1m or 1h5m.").
		HintOptions("500ms", "1s", "1m").
		Default("0").
		Short('s').
		DurationVar(sleep)
}

func initialiseProducer(kafkaParams *commands.KafkaParameters, verbosity internal.VerbosityLevel) (*kafka.Producer, error) {

	saramaLogWriter := ioutil.Discard
	if verbosity >= internal.Chatty {
		saramaLogWriter = os.Stdout
	}

	brokers := commands.GetBrokers(kafkaParams.Brokers)
	producer, err := kafka.NewProducer(
		brokers,
		kafka.WithClusterVersion(kafkaParams.Version),
		kafka.WithTLS(kafkaParams.TLS),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(kafkaParams.SASLMechanism,
			kafkaParams.SASLUsername,
			kafkaParams.SASLPassword,
			kafkaParams.SASLHandshakeVersion))

	if err != nil {
		return nil, err
	}
	return producer, nil
}

func produce(ctx context.Context,
	kafkaParams *commands.KafkaParameters,
	globalParams *commands.GlobalParameters,
	topic string,
	key, value string,
	serialize valueSerializer,
	count uint64,
	sleep time.Duration) error {
	producer, err := initialiseProducer(kafkaParams, globalParams.Verbosity)
	if err != nil {
		return err
	}

	defer func() {
		if globalParams.Verbosity >= internal.VeryVerbose {
			fmt.Println("Closing the kafka publisher.")
		}
		err := producer.Close()
		if err != nil {
			fmt.Println(format.Red("Failed to close the publisher", globalParams.EnableColor))
		}
	}()

	if globalParams.Verbosity >= internal.Verbose {
		msg := "message"
		switch {
		case count == 0:
			msg = "indefinite number of messages"
		case count == 1:
			msg = "a single message"
		case count > 1:
			msg = fmt.Sprintf("%d messages", count)
		}
		fmt.Printf("Publishing %s to Kafka\n", msg)
	}

	randomPk := len(key) == 0
	counter := uint64(1)
	capped := count > 0
	mustSleep := sleep > 0 && (count == 0 || count > 1)
	for {
		select {
		case <-ctx.Done():
			return nil
		default:
			if randomPk {
				key = fmt.Sprintf("%d%d", time.Now().UnixNano(), counter)
			}
			vBytes, err := serialize(value)
			if err != nil {
				return err
			}
			partition, offset, err := producer.Produce(topic, []byte(key), vBytes)
			if err != nil {
				return fmt.Errorf("failed to publish to kafka: %w", err)
			}
			if globalParams.Verbosity >= internal.VeryVerbose {
				fmt.Printf("Message has been published to the offset %d of partition %d (PK: %s).\n",
					offset,
					partition,
					key)
			}
			if capped && counter >= count {
				return nil
			}
			counter++
			if mustSleep {
				if globalParams.Verbosity >= internal.SuperVerbose {
					fmt.Printf("Waiting for %s before producing the next message.\n", sleep)
				}
				time.Sleep(sleep)
			}
		}
	}
}

func getValue(flagValue string) (string, error) {
	if !internal.IsEmpty(flagValue) {
		return flagValue, nil
	}
	value, err := readFromShellPipe()
	if err != nil {
		return "", err
	}
	if internal.IsEmpty(value) {
		return "", errors.New("the message content cannot be empty. Either pipe the content in or pass it as the second argument")
	}
	return value, nil
}

func readFromShellPipe() (string, error) {
	info, err := os.Stdin.Stat()
	if err != nil {
		return "", fmt.Errorf("failed to read the message content from shell: %w", err)
	}

	if info.Mode()&os.ModeCharDevice != 0 || info.Size() <= 0 {
		return "", nil
	}
	reader := bufio.NewReader(os.Stdin)
	var output []rune

	for {
		input, _, err := reader.ReadRune()
		if err != nil && err == io.EOF {
			break
		}
		output = append(output, input)
	}
	return strings.TrimRight(string(output), "\n"), nil
}
