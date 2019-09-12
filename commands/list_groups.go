package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"

	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/kafka"
)

type listGroups struct {
	globalParams   *GlobalParameters
	kafkaParams    *kafkaParameters
	includeMembers bool
	memberFilter   *regexp.Regexp
	groupFilter    *regexp.Regexp
	topics         []string
	format         string
}

func addListGroupsSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &listGroups{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("list", "Lists the consumer groups.").Action(cmd.run)
	c.Flag("members", "Enables fetching consumer group members.").
		Short('m').
		BoolVar(&cmd.includeMembers)
	c.Flag("topics", "The list of topics to retrieve the latest and the group offsets for.").
		Short('t').
		StringsVar(&cmd.topics)
	c.Flag("member-filter", "An optional regular expression to filter the member ID/Client/Host by.").
		Short('r').
		RegexpVar(&cmd.memberFilter)
	c.Flag("group-filter", "An optional regular expression to filter the groups by.").
		Short('g').
		RegexpVar(&cmd.groupFilter)
	c.Flag("format", "Sets the output format.").
		Default(tableFormat).
		Short('f').
		EnumVar(&cmd.format, plainTextFormat, tableFormat)
}

func (c *listGroups) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := initKafkaManager(c.globalParams, c.kafkaParams)

	if err != nil {
		return err
	}

	defer func() {
		manager.Close()
		cancel()
	}()

	return c.listGroups(ctx, manager)
}

func (c *listGroups) listGroups(ctx context.Context, manager *kafka.Manager) error {
	groups, err := manager.GetConsumerGroups(ctx, c.includeMembers, c.memberFilter, c.groupFilter, c.topics)
	if err != nil {
		return errors.Wrap(err, "Failed to list the brokers.")
	}

	if len(groups) == 0 {
		fmt.Println(getNotFoundMessage("consumer group", "group", c.groupFilter))
		return nil
	}

	switch c.format {
	case plainTextFormat:
		c.printPlainTextOutput(groups)
	case tableFormat:
		c.printTableOutput(groups)
	}
	return nil
}

func (*listGroups) printTableOutput(groups kafka.ConsumerGroups) {
	for name, group := range groups {
		groupTable := tablewriter.NewWriter(os.Stdout)
		groupTable.SetAutoWrapText(false)
		groupTable.SetAutoFormatHeaders(false)
		groupTable.SetHeader([]string{"Group: " + name})
		groupTable.SetColMinWidth(0, 80)
		if len(group.Members) > 0 {
			buff := bytes.Buffer{}
			buff.WriteString(fmt.Sprintf("\nMembers:\n"))
			table := tablewriter.NewWriter(&buff)
			table.SetHeader([]string{"Name", "Client ID", "Host"})
			for _, member := range group.Members {
				table.Append([]string{member.ID, member.ClientID, member.Host})
			}
			table.Render()
			groupTable.Append([]string{buff.String()})
		}

		if len(group.TopicOffsets) > 0 {
			buff := bytes.Buffer{}
			buff.WriteString(fmt.Sprintf("\nGroup Offsets:\n"))
			table := tablewriter.NewWriter(&buff)
			table.SetHeader([]string{"Partition", "Latest", "Current", "Lag"})
			table.SetColMinWidth(0, 20)
			table.SetColMinWidth(1, 20)
			table.SetColMinWidth(2, 20)
			table.SetColMinWidth(3, 20)
			table.SetAlignment(tablewriter.ALIGN_CENTER)

			for _, partitionOffsets := range group.TopicOffsets {
				partitions := partitionOffsets.SortPartitions()
				for _, partition := range partitions {
					offsets := partitionOffsets[int32(partition)]
					latest := strconv.FormatInt(offsets.Latest, 10)
					current := strconv.FormatInt(offsets.Current, 10)
					part := strconv.FormatInt(int64(partition), 10)
					table.Append([]string{part, latest, current, highlightLag(offsets.Lag())})
				}
			}
			table.Render()
			groupTable.Append([]string{buff.String()})
		}
		groupTable.SetHeaderLine(false)
		groupTable.Render()
	}
}

func (*listGroups) printPlainTextOutput(groups kafka.ConsumerGroups) {
	for name, group := range groups {
		fmt.Printf("%s: %s\n", bold("Group"), name)
		if len(group.Members) > 0 {
			fmt.Printf("\n%s\n\n", green("Members:"))
			for i, member := range group.Members {
				fmt.Printf("  %2d: %s\n", i+1, member)
			}
			fmt.Println()
		}

		if len(group.TopicOffsets) > 0 {
			fmt.Printf("\n%s\n\n", green("Partition Offsets:"))
			for _, partitionOffsets := range group.TopicOffsets {
				partitions := partitionOffsets.SortPartitions()
				for _, partition := range partitions {
					offsets := partitionOffsets[int32(partition)]
					fmt.Printf("  Partition %2d: %d out of %d (Lag: %s) \n", partition, offsets.Current, offsets.Latest, highlightLag(offsets.Lag()))
				}
			}
			fmt.Println()
		}
	}
}
