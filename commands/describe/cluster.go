package describe

import (
	"errors"
	"fmt"
	"sort"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

const (
	controlNodeFlag = "CTRL"
)

type cluster struct {
	globalParams *commands.GlobalParameters
	kafkaParams  *commands.KafkaParameters
	format       string
	style        string
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
		Short('c').
		BoolVar(&cmd.loadConfigs)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
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

	if len(meta.Brokers) == 0 {
		return errors.New("no brokers found")
	}

	sort.Sort(kafka.BrokersByID(meta.Brokers))

	switch c.format {
	case commands.JSONFormat:
		return output.PrintAsJSON(meta, c.style, c.globalParams.EnableColor)
	case commands.TableFormat:
		return c.printAsTable(meta)
	case commands.TreeFormat:
		return c.printAsList(meta, false)
	case commands.PlainTextFormat:
		return c.printAsList(meta, true)
	default:
		return nil
	}
}

func (c *cluster) printAsTable(meta *kafka.ClusterMetadata) error {
	table := tabular.NewTable(c.globalParams.EnableColor,
		tabular.C("ID").Align(tabular.AlignLeft),
		tabular.C("Address").Align(tabular.AlignLeft),
	)
	table.SetTitle(format.WithCount("Brokers", len(meta.Brokers)))
	for _, broker := range meta.Brokers {
		if broker.IsController {
			host := fmt.Sprintf("%v < %v",
				format.BoldGreen(broker.Host, c.globalParams.EnableColor),
				format.GreenLabel(controlNodeFlag, c.globalParams.EnableColor),
			)
			table.AddRow(format.BoldGreen(broker.ID, c.globalParams.EnableColor), host)
			continue
		}
		table.AddRow(broker.ID, broker.Host)
	}
	table.AddFooter("", fmt.Sprintf("Total: %d", len(meta.Brokers)))
	output.NewLines(1)
	table.Render()

	if len(meta.ConfigEntries) > 0 {
		output.NewLines(2)
		commands.PrintConfigTable(meta.ConfigEntries)
	}

	return nil
}

func (c *cluster) printAsList(meta *kafka.ClusterMetadata, plain bool) error {
	l := list.New(plain)
	if len(meta.Brokers) > 0 {
		l.AddItem("Brokers")
		l.Indent()
		for _, broker := range meta.Brokers {
			host := broker.String()
			if broker.IsController {
				host = fmt.Sprintf("%s %v", host, format.GreenLabel(controlNodeFlag, c.globalParams.EnableColor && !plain))
			}
			l.AddItem(host)
		}
		l.UnIndent()
	}

	if len(meta.ConfigEntries) > 0 {
		commands.PrintConfigList(l, meta.ConfigEntries, plain)
	}
	l.Render()
	return nil
}
