package kafka

import (
	"errors"
	"fmt"
	"log"

	"github.com/Shopify/sarama"
	"github.com/rcrowley/go-metrics"
)

func initClient(brokers []string, options ...Option) (sarama.Client, error) {
	if len(brokers) == 0 {
		return nil, errors.New("the brokers list cannot be empty")
	}
	ops := NewOptions()
	for _, option := range options {
		option(ops)
	}

	sarama.Logger = log.New(ops.logWriter, "KAFKA Client: ", log.LstdFlags)
	version, err := sarama.ParseKafkaVersion(ops.ClusterVersion)
	if err != nil {
		return nil, err
	}

	config := sarama.NewConfig()
	config.Version = version
	config.Consumer.Return.Errors = true
	config.ClientID = "Trubka"

	config.Producer.RequiredAcks = sarama.WaitForAll
	config.Producer.Return.Successes = true
	config.Producer.Partitioner = sarama.NewHashPartitioner

	metrics.UseNilMetrics = true
	if ops.sasl != nil {
		config.Net.SASL.Enable = true
		config.Net.SASL.Mechanism = ops.sasl.mechanism
		config.Net.SASL.User = ops.sasl.username
		config.Net.SASL.Password = ops.sasl.password
		config.Net.SASL.SCRAMClientGeneratorFunc = ops.sasl.client
	}

	if ops.TLS != nil {
		config.Net.TLS.Enable = true
		config.Net.TLS.Config = ops.TLS
	}

	client, err := sarama.NewClient(brokers, config)
	if err != nil {
		return nil, fmt.Errorf("failed to initialise the Kafka client: %w", err)
	}

	return client, nil
}
