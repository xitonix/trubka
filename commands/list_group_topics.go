package commands

//import (
//	"bytes"
//	"context"
//	"fmt"
//	"os"
//	"regexp"
//	"strconv"
//
//	"github.com/dustin/go-humanize"
//	"github.com/olekukonko/tablewriter"
//	"gopkg.in/alecthomas/kingpin.v2"
//
//	"github.com/xitonix/trubka/internal"
//	"github.com/xitonix/trubka/kafka"
//)
//
//type listGroupTopics struct {
//	globalParams   *GlobalParameters
//	kafkaParams    *KafkaParameters
//	includeOffsets bool
//	topicFilter    *regexp.Regexp
//	group          string
//	format         string
//}
//
//func addListGroupTopicsSubCommand(parent *kingpin.CmdClause, global *GlobalParameters, kafkaParams *KafkaParameters) {
//	cmd := &listGroupTopics{
//		globalParams: global,
//		kafkaParams:  kafkaParams,
//	}
//	c := parent.Command("topics", "Lists the topics a consumer group is subscribed to.").Action(cmd.run)
//	c.Arg("group", "The consumer group ID to fetch the topics for.").
//		Required().
//		StringVar(&cmd.group)
//
//	c.Flag("topic-filter", "An optional regular expression to filter the topics by.").
//		Short('t').
//		RegexpVar(&cmd.topicFilter)
//
//	c.Flag("offsets", "Enables fetching the offsets for the topics.").
//		Short('o').
//		BoolVar(&cmd.includeOffsets)
//
//	AddFormatFlag(c, &cmd.format)
//}
//
//func (c *listGroupTopics) run(_ *kingpin.ParseContext) error {
//	manager, ctx, cancel, err := InitKafkaManager(c.globalParams, c.kafkaParams)
//
//	if err != nil {
//		return err
//	}
//
//	defer func() {
//		manager.Close()
//		cancel()
//	}()
//
//	return c.listTopics(ctx, manager)
//}
//
//func (c *listGroupTopics) listTopics(ctx context.Context, manager *kafka.Manager) error {
//	topics, err := manager.GetGroupTopics(ctx, c.group, c.includeOffsets, c.topicFilter)
//	if err != nil {
//		return err
//	}
//
//	if len(topics) == 0 {
//		fmt.Println(GetNotFoundMessage("topic", "topic", c.topicFilter))
//		return nil
//	}
//
//	switch c.format {
//	case PlainTextFormat:
//		c.printPlainTextOutput(topics)
//	case TableFormat:
//		c.printTableOutput(topics)
//	}
//	return nil
//}
//
//func (c *listGroupTopics) printTableOutput(topics kafka.TopicPartitionOffset) {
//	for topic, partitionOffsets := range topics {
//		parentTable := tablewriter.NewWriter(os.Stdout)
//		parentTable.SetAutoWrapText(false)
//		parentTable.SetAutoFormatHeaders(false)
//		parentTable.SetHeader([]string{"Topic: " + topic})
//		parentTable.SetColMinWidth(0, 80)
//		if c.includeOffsets && len(partitionOffsets) > 0 {
//			buff := bytes.Buffer{}
//			table := tablewriter.NewWriter(&buff)
//			table.SetHeader([]string{"Partition", "Latest", "Current", "Lag"})
//			table.SetColMinWidth(0, 20)
//			table.SetColMinWidth(1, 20)
//			table.SetColMinWidth(2, 20)
//			table.SetColMinWidth(3, 20)
//			table.SetAlignment(tablewriter.ALIGN_CENTER)
//			partitions := partitionOffsets.SortPartitions()
//			for _, partition := range partitions {
//				offsets := partitionOffsets[int32(partition)]
//				latest := humanize.Comma(offsets.Latest)
//				current := humanize.Comma(offsets.Current)
//				part := strconv.FormatInt(int64(partition), 10)
//				table.Append([]string{part, latest, current, fmt.Sprint(HighlightLag(offsets.Lag(), c.globalParams.EnableColor))})
//			}
//			table.Render()
//			parentTable.Append([]string{buff.String()})
//		}
//		parentTable.SetHeaderLine(false)
//		parentTable.Render()
//	}
//}
//
//func (c *listGroupTopics) printPlainTextOutput(topics kafka.TopicPartitionOffset) {
//	for topic, partitionOffsets := range topics {
//		fmt.Printf("%s\n", internal.Bold(topic, c.globalParams.EnableColor))
//		if c.includeOffsets && len(partitionOffsets) > 0 {
//			fmt.Printf("\n\n")
//			partitions := partitionOffsets.SortPartitions()
//			for _, partition := range partitions {
//				offsets := partitionOffsets[int32(partition)]
//				fmt.Printf("   Partition %2d: %d out of %d (Lag: %s) \n", partition, offsets.Current, offsets.Latest,
//					HighlightLag(offsets.Lag(), c.globalParams.EnableColor))
//			}
//			fmt.Println()
//		}
//	}
//}
