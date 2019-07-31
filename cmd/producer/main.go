package main

import (
	"fmt"
	"os"
	"strconv"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
	"github.com/xitonix/flags"

	"github.com/xitonix/trubka/cmd/contracts"
)

func main() {
	brokers := flags.StringSlice("kafka-endpoints", "The comma separated list of Kafka endpoints in server:port format.").
		WithShort("k").
		WithDefault([]string{"127.0.0.1:9092"})

	topic := flags.String("topic", "The Kafka topic to publish to.").WithDefault("People")

	key := flags.String("partition-key", "Message partition key. It will be a random string if not specified.").
		WithShort("p")

	count := flags.Int("count", "Number of messages to publish").
		WithShort("c").
		WithDefault(1)

	flags.Parse()

	endpoints := brokers.Get()
	if len(endpoints) == 0 {
		exit(errors.New("brokers list cannot be empty"))
	}

	config := sarama.NewConfig()
	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	producer, err := sarama.NewSyncProducer(brokers.Get(), config)
	if err != nil {
		exit(errors.Wrap(err, "failed to create a new producer"))
	}

	defer func() {
		if err := producer.Close(); err != nil {
			exit(errors.Wrap(err, "failed to close the producer"))
		}
	}()

	num := count.Get()
	tp := topic.Get()
	for i := 0; i < num; i++ {
		pk, val, err := populate(key.Get())

		if err != nil {
			exit(err)
		}

		message := &sarama.ProducerMessage{
			Topic: tp,
			Key:   sarama.ByteEncoder(pk),
			Value: sarama.ByteEncoder(val)}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			exit(errors.Wrap(err, "failed to publish to Kafka"))
		}
		fmt.Printf("A new message has been published to %s partition %d, offset: %d\n", message.Topic, partition, offset)
	}
}

func exit(err error) {
	fmt.Printf("Err: %s\n", err)
	os.Exit(-1)
}

func populate(key string) ([]byte, []byte, error) {
	id := strconv.FormatInt(time.Now().UnixNano(), 10)

	if len(key) == 0 {
		key = id
	}

	person := contracts.Person{
		Id:     id,
		Name:   "John Doe",
		Age:    int32(time.Now().Second()),
		Gender: contracts.Gender_MALE,
	}

	val, err := proto.Marshal(&person)
	if err != nil {
		return nil, nil, errors.Wrap(err, "failed to marshal the message")
	}

	return []byte(key), val, nil
}
