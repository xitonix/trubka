package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"strconv"
	"syscall"

	"github.com/gookit/color"
	"github.com/olekukonko/tablewriter"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/kafka"
)

type groups struct {
	globalParams   *GlobalParameters
	kafkaParams    *kafkaParameters
	includeMembers bool
	memberFilter   *regexp.Regexp
	groupFilter    *regexp.Regexp
	topics         []string
	format         string
}

func addGroupsSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *kafkaParameters) {
	cmd := &groups{
		globalParams: global,
		kafkaParams:  kafkaParams,
	}
	c := parent.Command("groups", "Lists the consumer groups.").Action(cmd.run)
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

func (c *groups) run(_ *kingpin.ParseContext) error {
	manager, err := kafka.NewManager(c.kafkaParams.brokers,
		c.globalParams.Verbosity,
		kafka.WithClusterVersion(c.kafkaParams.version),
		kafka.WithTLS(c.kafkaParams.tls),
		kafka.WithClusterVersion(c.kafkaParams.version),
		kafka.WithSASL(c.kafkaParams.saslMechanism,
			c.kafkaParams.saslUsername,
			c.kafkaParams.saslPassword))

	if err != nil {
		return err
	}

	defer func() {
		if err := manager.Close(); err != nil {
			color.Error.Printf("Failed to close the Kafka client: %s", err)
		}
	}()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go func() {
		signals := make(chan os.Signal, 1)
		signal.Notify(signals, os.Kill, os.Interrupt, syscall.SIGTERM)
		<-signals
		cancel()
	}()

	return c.listGroups(ctx, manager)
}

func (c *groups) listGroups(ctx context.Context, manager *kafka.Manager) error {
	groups, err := manager.GetConsumerGroups(ctx, c.includeMembers, c.memberFilter, c.groupFilter, c.topics)
	if err != nil {
		return errors.Wrap(err, "Failed to list the brokers.")
	}

	if len(groups) == 0 {
		msg := "No consumer group has been found on the server."
		if c.groupFilter != nil {
			msg += fmt.Sprintf(" You might need to tweak the group filter (%s).", c.groupFilter.String())
		}
		fmt.Println(msg)
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

func (*groups) printTableOutput(groups kafka.ConsumerGroups) {
	for name, group := range groups {
		groupTable := tablewriter.NewWriter(os.Stdout)
		groupTable.SetAutoWrapText(false)
		groupTable.SetAutoFormatHeaders(false)
		groupTable.SetHeader([]string{"Group Name: " + name})
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

func (*groups) printPlainTextOutput(groups kafka.ConsumerGroups) {
	for name, group := range groups {
		color.Bold.Print("Group Name: ")
		fmt.Println(name)
		if len(group.Members) > 0 {
			color.Info.Println("\nMembers:\n")
			for i, member := range group.Members {
				fmt.Printf("  %2d: %s\n", i+1, member)
			}
			fmt.Println()
		}

		if len(group.TopicOffsets) > 0 {
			color.Info.Println("\nGroup Offsets:\n")
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
