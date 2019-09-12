package kafka

type BrokerMetadata struct {
	Version int
	Topics  []Topic
}
