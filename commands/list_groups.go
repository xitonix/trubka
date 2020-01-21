package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"

	"github.com/dustin/go-humanize"
	"github.com/olekukonko/tablewriter"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
)

type listGroups struct {
	globalParams   *GlobalParameters
	kafkaParams    *KafkaParameters
	includeMembers bool
	memberFilter   *regexp.Regexp
	groupFilter    *regexp.Regexp
	topics         string
	format         string
}

func addListGroupsSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *KafkaParameters) {
	cmd := &listGroups{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("list", "Lists the consumer groups.").Action(cmd.run)
	c.Flag("members", "Enables fetching consumer group members.").
		Short('m').
		BoolVar(&cmd.includeMembers)
	c.Flag("topics", "Comma separate list of the topics to retrieve the latest and the group offsets for.").
		Short('t').
		StringVar(&cmd.topics)
	c.Flag("member-filter", "An optional regular expression to filter the member ID/Client/Host by.").
		Short('r').
		RegexpVar(&cmd.memberFilter)
	c.Flag("group-filter", "An optional regular expression to filter the groups by.").
		Short('g').
		RegexpVar(&cmd.groupFilter)
	AddFormatFlag(c, &cmd.format)
}

func (c *listGroups) run(_ *kingpin.ParseContext) error {
	manager, ctx, cancel, err := InitKafkaManager(c.globalParams, c.kafkaParams)

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
	topics := strings.Split(c.topics, ",")
	groups, err := manager.GetConsumerGroups(ctx, c.includeMembers, c.memberFilter, c.groupFilter, topics)
	if err != nil {
		return fmt.Errorf("failed to list the brokers: %w", err)
	}

	if len(groups) == 0 {
		fmt.Println(GetNotFoundMessage("consumer group", "group", c.groupFilter))
		return nil
	}

	switch c.format {
	case PlainTextFormat:
		c.printPlainTextOutput(groups)
	case TableFormat:
		c.printTableOutput(groups)
	}
	return nil
}

func (c *listGroups) printTableOutput(groups kafka.ConsumerGroups) {
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
			table.SetHeader([]string{"", "ID", "Host"})
			for i, member := range group.Members {
				table.Append([]string{strconv.Itoa(i + 1), member.ID, member.Host})
			}
			table.Render()
			groupTable.Append([]string{buff.String()})
		}

		if len(group.TopicOffsets) > 0 {
			for topic, partitionOffsets := range group.TopicOffsets {
				buff := bytes.Buffer{}
				buff.WriteString(fmt.Sprintf("\nTopic: %s\n", topic))
				table := tablewriter.NewWriter(&buff)
				table.SetHeader([]string{"Partition", "Latest", "Current", "Lag"})
				table.SetColMinWidth(0, 20)
				table.SetColMinWidth(1, 20)
				table.SetColMinWidth(2, 20)
				table.SetColMinWidth(3, 20)
				table.SetAlignment(tablewriter.ALIGN_CENTER)
				partitions := partitionOffsets.SortPartitions()
				for _, partition := range partitions {
					offsets := partitionOffsets[int32(partition)]
					latest := humanize.Comma(offsets.Latest)
					current := humanize.Comma(offsets.Current)
					part := strconv.FormatInt(int64(partition), 10)
					table.Append([]string{part, latest, current, fmt.Sprint(highlightLag(offsets.Lag(), c.globalParams.EnableColor))})
				}
				table.Render()
				groupTable.Append([]string{buff.String()})
			}
		}
		groupTable.SetHeaderLine(false)
		groupTable.Render()
	}
}

func (c *listGroups) printPlainTextOutput(groups kafka.ConsumerGroups) {
	for name, group := range groups {
		fmt.Printf("%s: %s\n", internal.Bold("Group", c.globalParams.EnableColor), name)
		if len(group.Members) > 0 {
			fmt.Printf("\n%s\n\n", internal.Green("Members:", c.globalParams.EnableColor))
			for i, member := range group.Members {
				fmt.Printf("  %2d: %s\n", i+1, member)
			}
			fmt.Println()
		}

		if len(group.TopicOffsets) > 0 {
			fmt.Printf("\n%s\n\n", internal.Green("Partition Offsets:", c.globalParams.EnableColor))
			for topic, partitionOffsets := range group.TopicOffsets {
				partitions := partitionOffsets.SortPartitions()
				fmt.Printf("\n %s\n\n", internal.Green("Topic: "+topic, c.globalParams.EnableColor))
				for _, partition := range partitions {
					offsets := partitionOffsets[int32(partition)]
					fmt.Printf("   Partition %2d: %d out of %d (Lag: %s) \n", partition, offsets.Current,
						offsets.Latest, highlightLag(offsets.Lag(), c.globalParams.EnableColor))
				}
			}
			fmt.Println()
		}
	}
}
