package commands

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"sort"
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
		Short('f').
		RegexpVar(&cmd.memberFilter)
	c.Flag("group-filter", "An optional regular expression to filter the groups by.").
		Short('g').
		RegexpVar(&cmd.groupFilter)
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
				partitions := sortByPartitions(partitionOffsets)
				for _, partition := range partitions {
					offsets := partitionOffsets[int32(partition)]
					latest := strconv.FormatInt(offsets.Latest, 10)
					current := strconv.FormatInt(offsets.Current, 10)
					part := strconv.FormatInt(int64(partition), 10)
					lag := offsets.Lag()
					lagStr := "0"
					if lag > 0 {
						lagStr = color.Warn.Sprint(lag)
					}
					table.Append([]string{part, latest, current, lagStr})
				}
			}
			table.Render()
			groupTable.Append([]string{buff.String()})
		}
		groupTable.SetHeaderLine(false)
		groupTable.Render()
	}
	return nil
}

func sortByPartitions(p map[int32]kafka.GroupOffset) []int {
	sorted := make([]int, 0)
	if len(p) == 0 {
		return sorted
	}
	for partition := range p {
		sorted = append(sorted, int(partition))
	}
	sort.Ints(sorted)
	return sorted
}
