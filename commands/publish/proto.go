package publish

import (
	"fmt"

	"gopkg.in/alecthomas/kingpin.v2"

	"github.com/xitonix/trubka/commands"
	"github.com/xitonix/trubka/protobuf"
)

type proto struct {
	kafkaParams  *commands.KafkaParameters
	globalParams *commands.GlobalParameters
	message      string
	key          string
	topic        string
	proto        string
	count        uint32
	protoRoot    string
}

func addProtoSubCommand(parent *kingpin.CmdClause, global *commands.GlobalParameters, kafkaParams *commands.KafkaParameters) {
	cmd := &proto{
		kafkaParams:  kafkaParams,
		globalParams: global,
	}
	c := parent.Command("proto", "Publishes json/plain text messages to Kafka.").Action(cmd.run)
	c.Arg("topic", "The topic to publish to.").Required().StringVar(&cmd.topic)
	c.Arg("proto", "The proto to publish to.").Required().StringVar(&cmd.proto)
	c.Arg("content", "The message content. You can pipe the content in, or pass it as the command's second argument.").StringVar(&cmd.message)
	c.Flag("proto-root", "The path to the folder where your *.proto files live.").
		Short('r').
		Required().
		StringVar(&cmd.protoRoot)
	c.Flag("key", "The partition key of the message. If not set, a random value will be selected.").
		Short('k').
		StringVar(&cmd.key)
	c.Flag("count", "The number of messages to publish.").
		Default("1").
		Short('c').
		Uint32Var(&cmd.count)
}

func (c *proto) run(_ *kingpin.ParseContext) error {
	loader, err := protobuf.NewFileLoader(c.protoRoot)
	if err != nil {
		return err
	}

	err = loader.Load(c.proto)
	if err != nil {
		return err
	}

	msg, err := loader.Get(c.proto)
	if err != nil {
		return err
	}

	j, err := msg.MarshalJSONIndent()
	if err != nil {
		return err
	}
	fmt.Println(string(j))
	return nil

	//ctx, cancel := context.WithCancel(context.Background())
	//defer cancel()
	//go func() {
	//	internal.WaitForCancellationSignal()
	//	cancel()
	//}()
	//
	//if internal.IsEmpty(c.message) {
	//	msg, err := readFromShellPipe()
	//	if err != nil {
	//		return err
	//	}
	//	c.message = msg
	//}
	//
	//if internal.IsEmpty(c.message) {
	//	return errors.New("the message content cannot be empty. Either pipe the content in or pass it as the second argument")
	//}
	//
	//producer, err := initialiseProducer(c.kafkaParams, c.globalParams.Verbosity)
	//if err != nil {
	//	return err
	//}
	//
	//defer func() {
	//	if c.globalParams.Verbosity >= internal.VeryVerbose {
	//		fmt.Println("Closing the kafka publisher.")
	//	}
	//	err := producer.Close()
	//	if err != nil {
	//		fmt.Println(internal.Err("Failed to close the publisher", c.globalParams.EnableColor))
	//	}
	//}()
	//
	//if c.count == 0 {
	//	c.count = 1
	//}
	//msg := "message"
	//if c.count > 1 {
	//	msg = "messages"
	//}
	//fmt.Printf("Publishing %d %s to Kafka\n", c.count, msg)
	//for i := uint32(1); i <= c.count; i++ {
	//	select {
	//	case <-ctx.Done():
	//		return nil
	//	default:
	//		key := c.key
	//		if len(key) == 0 {
	//			key = fmt.Sprintf("%d%d", time.Now().UnixNano(), i)
	//		}
	//		partition, offset, err := producer.Produce(c.topic, []byte(key), []byte(c.message))
	//		if err != nil {
	//			return fmt.Errorf("failed to publish to kafka: %w", err)
	//		}
	//		if c.globalParams.Verbosity >= internal.Verbose {
	//			fmt.Printf("Message#%d has been published to the offset %d of partition %d (PK: %s)\n",
	//				i,
	//				offset,
	//				partition,
	//				key)
	//		}
	//	}
	//
	//}
	//return nil
}
