package describe

import (
	"fmt"
	"os"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/tabular"
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
		NoEnvar().
		Short('C').
		BoolVar(&cmd.loadConfigs)
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
	table := tabular.NewTable(os.Stdout, c.globalParams.EnableColor,
		tabular.C("ID").Align(tabular.AlignLeft),
		tabular.C("Address").Align(tabular.AlignLeft).FAlign(tabular.AlignRight))
	table.SetTitle("Brokers (%d)", len(meta.Brokers))
	for _, broker := range meta.Brokers {
		host := broker.Host
		if broker.IsController {
			host += fmt.Sprintf(" [%v]", format.Bold("C", c.globalParams.EnableColor))
		}
		table.AddRow(broker.ID, host)
	}
	table.AddFooter("", fmt.Sprintf("Total: %d", len(meta.Brokers)))
	table.SetCaption("[C]: Controller Node")
	table.Render()

	if len(meta.ConfigEntries) > 0 {
		fmt.Println()
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
