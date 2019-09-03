package commands

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"regexp"
	"syscall"

	"github.com/gookit/color"
	"github.com/pkg/errors"
	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/kafka"
)

type groups struct {
	globalParams   *GlobalParameters
	kafkaParams    *kafkaParameters
	includeMembers bool
	memberFilter   *regexp.Regexp
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
	groups, err := manager.GetConsumerGroups(ctx, c.includeMembers, c.memberFilter, c.topics)
	if err != nil {
		return errors.Wrap(err, "Failed to list the brokers.")
	}
	for name, group := range groups {
		fmt.Println(name)
		fmt.Printf("%+v\n", group.GroupOffsets)
	}
	//if len(br) == 0 {
	//	return errors.New("No broker found")
	//}
	//table := tablewriter.NewWriter(os.Stdout)
	//headers := []string{"ID", "Address"}
	//if c.includeMetadata {
	//	headers = append(headers, "Version", "Topic (No. of Partitions)")
	//}
	//table.SetHeader(headers)
	//table.SetAlignment(tablewriter.ALIGN_LEFT)
	//table.SetAutoWrapText(false)
	//for _, broker := range br {
	//	row := []string{strconv.Itoa(broker.ID), broker.Address}
	//	if c.includeMetadata && len(broker.Meta.Topics) > 0 {
	//		topics := make([]string, len(broker.Meta.Topics))
	//		for i, topic := range broker.Meta.Topics {
	//			topics[i] = fmt.Sprintf("%s (%d)", topic.Name, topic.NumberOdPartitions)
	//		}
	//		row = append(row,
	//			strconv.Itoa(broker.Meta.Version),
	//			strings.Join(topics, "\n"))
	//	}
	//	table.Append(row)
	//}
	//table.Render()
	return nil
}
