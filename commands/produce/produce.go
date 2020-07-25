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
	count uint32) error {
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

	if count == 0 {
		count = 1
	}
	msg := "message"
	if count > 1 {
		msg = "messages"
	}

	if globalParams.Verbosity >= internal.Verbose {
		fmt.Printf("Publishing %d %s to Kafka\n", count, msg)
	}

	randomPk := len(key) == 0
	for i := uint32(1); i <= count; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			if randomPk {
				key = fmt.Sprintf("%d%d", time.Now().UnixNano(), i)
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
				fmt.Printf("Message#%d has been published to the offset %d of partition %d (PK: %s)\n",
					i,
					offset,
					partition,
					key)
			}
		}

	}
	return nil
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
