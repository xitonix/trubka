package commands

import (
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/olekukonko/tablewriter"

	"github.com/xitonix/trubka/kafka"
)

type listBrokers struct {
	globalParams    *GlobalParameters
	kafkaParams     *kafkaParameters
	includeMetadata bool
}

func addListBrokersSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &listBrokers{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("list", "Lists the brokers in the Kafka cluster.").Action(cmd.run)
	c.Flag("metadata", "Enables fetching metadata for each broker.").Short('m').BoolVar(&cmd.includeMetadata)
}

func (c *listBrokers) run(_ *kingpin.ParseContext) error {
	//TODO: Add plain text output format
	manager, ctx, cancel, err := initKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	return c.listBrokers(ctx, manager)
}

func (c *listBrokers) listBrokers(ctx context.Context, manager *kafka.Manager) error {
	br, err := manager.GetBrokers(ctx, c.includeMetadata)
	if err != nil {
		return errors.Wrap(err, "Failed to list the brokers.")
	}
	if len(br) == 0 {
		return errors.New("No broker found")
	}
	table := tablewriter.NewWriter(os.Stdout)
	headers := []string{"ID", "Address"}
	if c.includeMetadata {
		headers = append(headers, "Version", "Topic (No. of Partitions)")
	}
	table.SetHeader(headers)
	table.SetAlignment(tablewriter.ALIGN_LEFT)
	table.SetAutoWrapText(false)
	for _, broker := range br {
		row := []string{strconv.Itoa(broker.ID), broker.Address}
		if c.includeMetadata && len(broker.Meta.Topics) > 0 {
			topics := make([]string, len(broker.Meta.Topics))
			for i, topic := range broker.Meta.Topics {
				topics[i] = fmt.Sprintf("%s (%d)", topic.Name, topic.NumberOdPartitions)
			}
			row = append(row,
				strconv.Itoa(broker.Meta.Version),
				strings.Join(topics, "\n"))
		}
		table.Append(row)
	}
	table.Render()
	return nil
}
