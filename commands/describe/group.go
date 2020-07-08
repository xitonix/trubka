package describe

import (
	"fmt"
	"strings"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal/output"
	"github.com/xitonix/trubka/internal/output/format"
	"github.com/xitonix/trubka/internal/output/format/list"
	"github.com/xitonix/trubka/internal/output/format/tabular"
	"github.com/xitonix/trubka/kafka"
)

type group struct {
	kafkaParams    *commands.KafkaParameters
	globalParams   *commands.GlobalParameters
	includeMembers bool
	group          string
	format         string
	style          string
}

func addGroupSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &group{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("group", "Describes a consumer group.").Action(cmd.run)
	c.Arg("group", "The consumer group name to describe.").Required().StringVar(&cmd.group)
	c.Flag("include-members", "Lists the group members and partition assignments in the output.").
		NoEnvar().
		Short('m').
		BoolVar(&cmd.includeMembers)
	commands.AddFormatFlag(c, &cmd.format, &cmd.style)
}

func (c *group) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := commands.InitKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	cgd, err := manager.DescribeGroup(ctx, c.group, c.includeMembers)
	if err != nil {
		return err
	}

	switch c.format {
	case commands.JsonFormat:
		data := cgd.ToJson(c.includeMembers)
		return output.PrintAsJson(data, c.style, c.globalParams.EnableColor)
	case commands.TableFormat:
		return c.printAsTable(cgd)
	case commands.TreeFormat:
		return c.printAsList(cgd, false)
	case commands.PlainTextFormat:
		return c.printAsList(cgd, true)
	}
	return nil
}

func (c *group) printAsList(details *kafka.ConsumerGroupDetails, plain bool) error {
	l := list.New(plain)
	l.AddItemF("Coordinator: %s", details.Coordinator.String())
	l.AddItemF("      State: %s", format.GroupStateLabel(details.State, c.globalParams.EnableColor && !plain))
	l.AddItemF("   Protocol: %s/%s", details.Protocol, details.ProtocolType)
	if c.includeMembers && len(details.Members) > 0 {
		l.AddItemF("Members")
		l.Indent()
		for member, md := range details.Members {
			l.AddItemF("%s (%s)", member, md.ClientHost)
			if len(details.Members[member].Assignments) == 0 {
				continue
			}
			tps := details.Members[member].Assignments
			sortedTopics := tps.SortedTopics()
			l.Indent()
			for _, topic := range sortedTopics {
				l.AddItemF("%s: %s", topic, tps.SortedPartitionsString(topic))
			}
			l.UnIndent()
		}
		l.UnIndent()
	}
	l.Render()
	return nil
}

func (c *group) printAsTable(details *kafka.ConsumerGroupDetails) error {
	table := tabular.NewTable(c.globalParams.EnableColor,
		tabular.C("Coordinator"),
		tabular.C("State"),
		tabular.C("Protocol"),
		tabular.C("Protocol Type"),
	)

	table.AddRow(
		details.Coordinator.Host,
		format.GroupStateLabel(details.State, c.globalParams.EnableColor),
		details.Protocol,
		details.ProtocolType,
	)
	table.Render()

	if c.includeMembers && len(details.Members) > 0 {
		c.printMemberDetailsTable(details.Members)
	}
	return nil
}

func (c *group) printMemberDetailsTable(members map[string]*kafka.GroupMemberDetails) {
	table := tabular.NewTable(c.globalParams.EnableColor,
		tabular.C("ID").HAlign(tabular.AlignLeft).FAlign(tabular.AlignRight),
		tabular.C("Client Host"),
		tabular.C("Assignments").Align(tabular.AlignLeft),
	)

	table.SetTitle(format.WithCount("Members", len(members)))
	for name, desc := range members {
		sortedTopics := desc.Assignments.SortedTopics()
		var buf strings.Builder
		for i, topic := range sortedTopics {
			buf.WriteString(format.Underline(topic))
			partitions := desc.Assignments.SortedPartitions(topic)
			for j, p := range partitions {
				if j%20 == 0 {
					buf.WriteString("\n")
				}
				buf.WriteString(fmt.Sprintf("%d ", p))
			}
			if i < len(sortedTopics)-1 {
				buf.WriteString("\n\n")
			}
		}
		table.AddRow(
			format.SpaceIfEmpty(name),
			format.SpaceIfEmpty(desc.ClientHost),
			format.SpaceIfEmpty(buf.String()),
		)
	}
	table.AddFooter(fmt.Sprintf("Total: %d", len(members)), " ", " ")
	table.Render()
}
