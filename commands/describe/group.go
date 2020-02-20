package describe

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/internal/output"
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
	case commands.PlainTextFormat:
		c.printPlainTextOutput(cgd)
	case commands.TableFormat:
		c.printTableOutput(cgd)
	}
	return nil
}

func (c *group) printPlainTextOutput(details *kafka.ConsumerGroupDetails) {
	fmt.Println(details.String())
	if c.includeMembers {
		output.UnderlineWithCount("Members", len(details.Members))
		for member, md := range details.Members {
			fmt.Println("  ID: " + member)
			fmt.Printf("HOST: %s\n\n", md.ClientHost)
			if len(details.Members[member].TopicPartitions) == 0 {
				continue
			}
			tps := details.Members[member].TopicPartitions
			sortedTopics := tps.SortedTopics()
			output.UnderlineWithCount("Assignments", len(sortedTopics))
			for _, topic := range sortedTopics {
				space := strings.Repeat(" ", 2)
				fmt.Printf("%s- %s: %s\n", space, topic, tps.SortedPartitionsString(topic))
			}
			fmt.Println()
		}
	}
}

func (c *group) printTableOutput(details *kafka.ConsumerGroupDetails) {
	table := output.InitStaticTable(os.Stdout,
		output.H("Coordinator", tablewriter.ALIGN_CENTER),
		output.H("State", tablewriter.ALIGN_CENTER),
		output.H("Protocol", tablewriter.ALIGN_CENTER),
		output.H("Protocol Type", tablewriter.ALIGN_CENTER),
	)
	table.Append([]string{details.Coordinator.Address,
		internal.HighlightGroupState(details.State, c.globalParams.EnableColor),
		details.Protocol,
		details.ProtocolType},
	)
	table.Render()

	if c.includeMembers {
		c.printMemberDetailsTable(details.Members)
	}
}

func (c *group) printMemberDetailsTable(members map[string]*kafka.GroupMemberDetails) {
	table := tablewriter.NewWriter(os.Stdout)
	table = output.InitStaticTable(os.Stdout,
		output.H("ID", tablewriter.ALIGN_LEFT),
		output.H("Client Host", tablewriter.ALIGN_CENTER),
		output.H("Assignments", tablewriter.ALIGN_CENTER),
	)

	output.WithCount("Members", len(members))
	rows := make([][]string, 0)
	for name, desc := range members {
		var buf bytes.Buffer
		inner := output.InitStaticTable(&buf,
			output.H("Topic", tablewriter.ALIGN_LEFT),
			output.H("Partition", tablewriter.ALIGN_CENTER),
		)
		sortedTopics := desc.TopicPartitions.SortedTopics()
		for _, topic := range sortedTopics {
			inner.Append([]string{
				output.SpaceIfEmpty(topic),
				output.SpaceIfEmpty(desc.TopicPartitions.SortedPartitionsString(topic)),
			})
		}
		inner.Render()
		row := []string{
			output.SpaceIfEmpty(name),
			output.SpaceIfEmpty(desc.ClientHost),
			output.SpaceIfEmpty(buf.String()),
		}
		rows = append(rows, row)
	}
	table.AppendBulk(rows)
	table.SetFooter([]string{fmt.Sprintf("Total: %d", len(members)), " ", " "})
	table.SetFooterAlignment(tablewriter.ALIGN_RIGHT)
	table.Render()
}
