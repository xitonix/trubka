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
	case commands.ListFormat:
		return c.printAsList(cgd, false)
	case commands.PlainTextFormat:
		return c.printAsList(cgd, true)
	}
	return nil
}

func (c *group) printAsList(details *kafka.ConsumerGroupDetails, plain bool) error {
	c.printGroupDetails(details, plain)

	if c.includeMembers && len(details.Members) > 0 {
		output.NewLines(2)
		b := list.New(plain)
		b.AsTree()
		b.SetTitle(format.WithCount("Members", len(details.Members)))
		for member, md := range details.Members {
			b.AddItemF("%s (%s)", member, md.ClientHost)
			if len(details.Members[member].TopicPartitions) == 0 {
				continue
			}
			tps := details.Members[member].TopicPartitions
			sortedTopics := tps.SortedTopics()
			b.Intend()
			for _, topic := range sortedTopics {
				b.AddItemF("%s: %s", topic, tps.SortedPartitionsString(topic))
			}
			b.UnIntend()
		}
		b.Render()
	}
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

func (c *group) printGroupDetails(details *kafka.ConsumerGroupDetails, plain bool) {
	fmt.Printf("       Name: %s\nCoordinator: %s\n      State: %s\n   Protocol: %s-%s",
		details.Name,
		details.Coordinator.Host,
		format.GroupStateLabel(details.State, c.globalParams.EnableColor && !plain),
		details.Protocol,
		details.ProtocolType)
}

func (c *group) printMemberDetailsTable(members map[string]*kafka.GroupMemberDetails) {
	table := tabular.NewTable(c.globalParams.EnableColor,
		tabular.C("ID").HAlign(tabular.AlignLeft).FAlign(tabular.AlignRight),
		tabular.C("Client Host"),
		tabular.C("Assignments").Align(tabular.AlignLeft),
	)

	table.SetTitle(format.WithCount("Members", len(members)))
	for name, desc := range members {
		sortedTopics := desc.TopicPartitions.SortedTopics()
		var buf strings.Builder
		for i, topic := range sortedTopics {
			buf.WriteString(format.Underline(topic))
			partitions := desc.TopicPartitions.SortedPartitions(topic)
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
