package list

import (
	"errors"
	"fmt"
	"os"
	"sort"
	"strconv"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/olekukonko/tablewriter"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/kafka"
)

type brokers struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters
	format       string
}

func addBrokersSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &brokers{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("brokers", "Lists the brokers in the Kafka cluster.").Action(cmd.run)
	commands.AddFormatFlag(c, &cmd.format)
}

func (c *brokers) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	brokers, err := manager.GetBrokers(ctx)
	if err != nil {
		return fmt.Errorf("failed to list the brokers: %w", err)
	}

	if len(brokers) == 0 {
		return errors.New("no broker found")
	}

	sort.Sort(kafka.BrokersById(brokers))

	switch c.format {
	case commands.PlainTextFormat:
		c.printPlainTextOutput(brokers)
	case commands.TableFormat:
		c.printTableOutput(brokers)
	}
	return nil
}

func (c *brokers) printTableOutput(brokers []*kafka.Broker) {
	table := output.InitStaticTable(os.Stdout,
		output.H("ID", tablewriter.ALIGN_LEFT),
		output.H("Address", tablewriter.ALIGN_LEFT),
	)
	for _, broker := range brokers {
		id := strconv.FormatInt(int64(broker.ID), 10)
		host := broker.Host
		if broker.IsController {
			host += fmt.Sprintf(" [%s]", internal.Bold("C", c.globalParams.EnableColor))
		}
		row := []string{id, host}
		table.Append(row)
	}
	table.SetFooter([]string{" ", fmt.Sprintf("Total: %d", len(brokers))})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
	c.printLegend()
}

func (c *brokers) printPlainTextOutput(brokers []*kafka.Broker) {
	fmt.Printf("\n%s\n", output.UnderlineWithCount("Brokers", len(brokers)))
	for _, broker := range brokers {
		fmt.Printf("%s\n", broker.String())
	}
	c.printLegend()

}

func (*brokers) printLegend() {
	fmt.Println("[C]: Controller Node")
}
