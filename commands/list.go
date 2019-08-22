package commands

import (
	"context"
	"fmt"
	"io/ioutil"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"strings"
	"syscall"

	"github.com/gookit/color"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/olekukonko/tablewriter"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

const (
	brokers = "brokers"
)

type list struct {
	params       *Parameters
	protoRoot    string
	topicFilter  *regexp.Regexp
	manager      *kafka.Manager
	toList       string
	validTargets []string
}

// AddList initialises the list command and adds it to the application.
func AddList(app *kingpin.Application, params *Parameters) {
	cmd := &list{
		params:       params,
		validTargets: []string{brokers},
	}
	usage := fmt.Sprintf("Queries the information about the specified target (%s).", strings.Join(cmd.validTargets, ","))
	c := app.Command("list", usage).Action(cmd.run)
	c.Arg("target", "The query target.").
		Required().
		HintOptions(cmd.validTargets...).
		EnumVar(&cmd.toList, cmd.validTargets...)
}

func (c *list) run(_ *kingpin.ParseContext) error {
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

	c.manager = manager

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

	switch c.toList {
	case brokers:
		err = c.listBrokers(ctx)
	default:
		err = errors.New("Invalid target defined.")
	}

	return err
}

func (c *list) listBrokers(ctx context.Context) error {
	br, err := c.manager.GetBrokers(ctx)
	if err != nil {
		return errors.Wrap(err, "Failed to list the brokers.")
	}
	if len(br) == 0 {
		return errors.New("No broker found")
	}
	table := tablewriter.NewWriter(os.Stdout)
	table.SetHeader([]string{"ID", "Address", "Rack"})
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	for _, broker := range br {
		table.Append([]string{strconv.Itoa(broker.ID), broker.Address, broker.Rack})
	}
	table.Render()
	return nil
}
