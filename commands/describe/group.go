package describe

import (
	"fmt"
	"os"

	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
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
	//var totalPartitions int64
	//for _, topic := range topics {
	//	totalPartitions += int64(topic.NumberOfPartitions)
	//	fmt.Printf("%s\n", topic)
	//}
	//fmt.Println("\nTotal\n-----")
	//fmt.Printf("Number of topics: %s\n", humanize.Comma(int64(len(topics))))
	//fmt.Printf("Number of partitions: %s", humanize.Comma(totalPartitions))
}

func (c *group) printTableOutput(details *kafka.ConsumerGroupDetails) {
	table := commands.InitStaticTable(os.Stdout, map[string]int{
		"Coordinator":   tablewriter.ALIGN_CENTER,
		"State":         tablewriter.ALIGN_CENTER,
		"Protocol":      tablewriter.ALIGN_CENTER,
		"Protocol Type": tablewriter.ALIGN_CENTER,
	})
	table.Append([]string{details.Coordinator.Address, details.State, details.Protocol, details.ProtocolType})
	table.Render()

	if c.includeMembers {
		c.printMemberDetailsTable(details.Members)
	}
}

func (c *group) printMemberDetailsTable(members map[string]*kafka.GroupMemberDetails) {
	fmt.Println("\nMembers:")
	table := tablewriter.NewWriter(os.Stdout)
	table = commands.InitStaticTable(os.Stdout, map[string]int{
		"ID":          tablewriter.ALIGN_LEFT,
		"Client Host": tablewriter.ALIGN_CENTER,
		"Assignments": tablewriter.ALIGN_CENTER,
	})
	rows := make([][]string, 0)
	for name, desc := range members {
		//TODO: Print the Assignments
		row := []string{name, desc.ClientHost, " "}
		rows = append(rows, row)
	}
	table.AppendBulk(rows)
	table.Render()
}
