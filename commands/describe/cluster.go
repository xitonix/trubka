package describe

import (
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

type cluster struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters
	format       string
	loadConfigs  bool
}

func addClusterSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &cluster{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("cluster", "Describes the Kafka cluster.").Action(cmd.run)
	c.Flag("load-config", "Loads the cluster's configurations from the server.").
		Short('C').BoolVar(&cmd.loadConfigs)
	commands.AddFormatFlag(c, &cmd.format)
}

func (c *cluster) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	meta, err := manager.DescribeCluster(ctx, c.loadConfigs)
	if err != nil {
		return fmt.Errorf("failed to list the brokers: %w", err)
	}

	sort.Sort(kafka.BrokersById(meta.Brokers))
	sort.Sort(kafka.ConfigEntriesByName(meta.ConfigEntries))

	switch c.format {
	case commands.PlainTextFormat:
		c.printPlainTextOutput(meta)
	case commands.TableFormat:
		c.printTableOutput(meta)
	}
	return nil
}

func (c *cluster) printTableOutput(meta *kafka.ClusterMetadata) {
	table := output.InitStaticTable(os.Stdout,
		output.H("ID", tablewriter.ALIGN_LEFT),
		output.H("Address", tablewriter.ALIGN_LEFT),
	)
	output.WithCount("Brokers", len(meta.Brokers))
	for _, broker := range meta.Brokers {
		id := strconv.FormatInt(int64(broker.ID), 10)
		host := broker.Host
		if broker.IsController {
			host += fmt.Sprintf(" [%s]", internal.Bold("C", c.globalParams.EnableColor))
		}
		row := []string{id, host}
		table.Append(row)
	}
	table.SetFooter([]string{" ", fmt.Sprintf("Total: %d", len(meta.Brokers))})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
	c.printLegend()

	if len(meta.ConfigEntries) > 0 {
		commands.PrintConfigTable(meta.ConfigEntries)
	}
}

func (c *cluster) printPlainTextOutput(meta *kafka.ClusterMetadata) {
	output.UnderlineWithCount("Brokers", len(meta.Brokers))
	for _, broker := range meta.Brokers {
		fmt.Printf("%s\n", broker.String())
	}
	c.printLegend()

	if len(meta.ConfigEntries) > 0 {
		commands.PrintConfigPlain(meta.ConfigEntries)
	}
}

func (*cluster) printLegend() {
	fmt.Println("[C]: Controller Node")
}
