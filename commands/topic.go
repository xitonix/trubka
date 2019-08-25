package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"syscall"

	"github.com/gookit/color"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type topic struct {
	params    *Parameters
	topicName string
}

func addTopicSubCommand(parent *kingpin.CmdClause, params *Parameters) {
	cmd := &topic{
		params: params,
	}
	c := parent.Command("topics", "Loads the existing topics from the server.").Action(cmd.run)
	c.Arg("topic", "The topic to query.").Required().StringVar(&cmd.topicName)
}

func (c *topic) run(_ *kingpin.ParseContext) error {
	saramaLogWriter := ioutil.Discard
	if c.params.Verbosity >= internal.Chatty {
		saramaLogWriter = os.Stdout
	}

	manager, err := kafka.NewManager(c.params.Brokers,
		kafka.WithClusterVersion(c.params.KafkaVersion),
		kafka.WithTLS(c.params.TLS),
		kafka.WithClusterVersion(c.params.KafkaVersion),
		kafka.WithTLS(c.params.TLS),
		kafka.WithLogWriter(saramaLogWriter),
		kafka.WithSASL(c.params.SASLMechanism, c.params.SASLUsername, c.params.SASLPassword))

	if err != nil {
		return err
	}

	defer func() {
		if err := manager.Close(); err != nil {
			color.Error.Printf("Failed to close the Kafka client: %s", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		cancel()
	}()

	topics, err := manager.GetTopics(ctx, c.filter)
	if err != nil {
		return err
	}

	if len(topics) == 0 {
		fmt.Println("No topics found!")
		return nil
	}

	for i, topic := range topics {
		fmt.Printf("%d. %s\n", i+1, topic)
	}
	return nil
}
