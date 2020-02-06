package produce

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"strings"
	"time"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
)

type plain struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	message      string
	key          string
	topic        string
	count        uint32
}

func addPlainSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &plain{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("plain", "Publishes json/plain text messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("content", "The message content. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	c.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(&cmd.key)
	c.Flag("count", "The number of messages to publish.").
		Default("1").
		Short('c').
		Uint32Var(&cmd.count)
}

func (c *plain) run(_ *kingpin.ParseContext) error {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() {
		internal.WaitForCancellationSignal()
		cancel()
	}()

	if internal.IsEmpty(c.message) {
		msg, err := readFromShellPipe()
		if err != nil {
			return err
		}
		c.message = msg
	}

	if internal.IsEmpty(c.message) {
		return errors.New("the message content cannot be empty. Either pipe the content in or pass it as the second argument")
	}

	producer, err := initialiseProducer(c.kafkaParams, c.globalParams.Verbosity)
	if err != nil {
		return err
	}

	defer func() {
		if c.globalParams.Verbosity >= internal.VeryVerbose {
			fmt.Println("Closing the kafka publisher.")
		}
		err := producer.Close()
		if err != nil {
			fmt.Println(internal.Err("Failed to close the publisher", c.globalParams.EnableColor))
		}
	}()

	if c.count == 0 {
		c.count = 1
	}
	msg := "message"
	if c.count > 1 {
		msg = "messages"
	}
	fmt.Printf("Publishing %d %s to Kafka\n", c.count, msg)
	for i := uint32(1); i <= c.count; i++ {
		select {
		case <-ctx.Done():
			return nil
		default:
			key := c.key
			if len(key) == 0 {
				key = fmt.Sprintf("%d%d", time.Now().UnixNano(), i)
			}
			partition, offset, err := producer.Produce(c.topic, []byte(key), []byte(c.message))
			if err != nil {
				return fmt.Errorf("failed to publish to kafka: %w", err)
			}
			if c.globalParams.Verbosity >= internal.Verbose {
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
