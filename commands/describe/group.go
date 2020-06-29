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
	commands.AddFormatFlag(c, &cmd.format)
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
	case commands.ListFormat:
		c.printListOutput(cgd)
	case commands.TableFormat:
		c.printTableOutput(cgd)
	}
	return nil
}

func (c *group) printListOutput(details *kafka.ConsumerGroupDetails) {

	fmt.Printf("         Name: %s\n  Coordinator: %s\n        State: %s\n     Protocol: %s\nProtocol Type: %s",
		details.Name,
		details.Coordinator.Host,
		format.GroupStateLabel(details.State, c.globalParams.EnableColor),
		details.Protocol,
		details.ProtocolType)

	if c.includeMembers && len(details.Members) > 0 {
		output.NewLines(2)
		fmt.Println(format.UnderlinedTitleWithCount("Members", len(details.Members)))
		for member, md := range details.Members {
			fmt.Println("  ID: " + member)
			fmt.Printf("HOST: %s\n\n", md.ClientHost)
			if len(details.Members[member].TopicPartitions) == 0 {
				continue
			}
			tps := details.Members[member].TopicPartitions
			sortedTopics := tps.SortedTopics()
			fmt.Println(format.UnderlinedTitleWithCount("Assignments", len(sortedTopics)))
			b := list.NewBullet()
			b.AsTree()
			for _, topic := range sortedTopics {
				b.AddItem(topic)
				b.Intend()
				b.AddItem(tps.SortedPartitionsString(topic))
				b.UnIntend()
			}
			b.Render()
			output.NewLines(1)
		}
	}
}

func (c *group) printTableOutput(details *kafka.ConsumerGroupDetails) {
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
