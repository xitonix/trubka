package describe

import (
	"fmt"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

const (
	describeClusterCaption = " [C]: Controller Node"
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
	table := tabular.NewTable(c.globalParams.EnableColor,
		tabular.C("ID").Align(tabular.AlignLeft),
		tabular.C("Address").Align(tabular.AlignLeft),
	)
	table.SetTitle(format.TitleWithCount("Brokers", len(meta.Brokers)))
	for _, broker := range meta.Brokers {
		host := broker.Host
		if broker.IsController {
			host += fmt.Sprintf(" [%v]", format.Bold("C", c.globalParams.EnableColor))
		}
		table.AddRow(broker.ID, host)
	}
	table.AddFooter("", fmt.Sprintf("Total: %d", len(meta.Brokers)))
	table.SetCaption(describeClusterCaption)
	table.Render()

	if len(meta.ConfigEntries) > 0 {
		internal.NewLines(1)
		commands.PrintConfigTable(meta.ConfigEntries)
	}
}

func (c *cluster) printPlainTextOutput(meta *kafka.ClusterMetadata) {
	b := list.NewBullet()
	b.SetTitle(format.TitleWithCount("Brokers", len(meta.Brokers)))
	for _, broker := range meta.Brokers {
		b.AddItem(broker.String())
	}
	b.SetCaption(describeClusterCaption)
	b.Render()

	if len(meta.ConfigEntries) > 0 {
		internal.NewLines(2)
		commands.PrintConfigPlain(meta.ConfigEntries)
	}
}
