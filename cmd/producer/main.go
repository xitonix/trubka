package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Shopify/sarama"
	"github.com/golang/protobuf/proto"
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

	format := flags.String("format", "The message serialisation format.").
		WithShort("f").WithValidRange(true, "proto", "json", "plain").
		WithDefault("proto")

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
	config.Version = sarama.MaxVersion
	producer, err := sarama.NewSyncProducer(brokers.Get(), config)
	if err != nil {
		exit(fmt.Errorf("failed to create a new producer: %w", err))
	}

	defer func() {
		if err := producer.Close(); err != nil {
			exit(fmt.Errorf("failed to close the producer: %w", err))
		}
	}()

	num := count.Get()
	tp := topic.Get()
	for i := 0; i < num; i++ {
		pk, val, err := populate(key.Get(), strings.ToLower(format.Get()))

		if err != nil {
			exit(err)
		}

		message := &sarama.ProducerMessage{
			Topic: tp,
			Key:   sarama.ByteEncoder(pk),
			Value: sarama.ByteEncoder(val)}

		partition, offset, err := producer.SendMessage(message)
		if err != nil {
			exit(fmt.Errorf("failed to publish to Kafka: %w", err))
		}
		fmt.Printf("A new message has been published to %s partition %d, offset: %d\n", message.Topic, partition, offset)
	}
}

func exit(err error) {
	fmt.Printf("Err: %s\n", err)
	os.Exit(-1)
}

func populate(key string, format string) ([]byte, []byte, error) {
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

	var val []byte
	var err error

	switch strings.TrimSpace(format) {
	case "proto":
		val, err = proto.Marshal(&person)
	case "json":
		val, err = json.Marshal(&person)
	default:
		val = []byte(fmt.Sprintf("%s|%d (%s)", person.Name, person.Age, person.Id))
	}

	if err != nil {
		return nil, nil, fmt.Errorf("failed to marshal the message: %w", err)
	}

	return []byte(key), val, nil
}
