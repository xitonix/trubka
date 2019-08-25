package kafka

type Broker struct {
	Address string
	ID      int
	Meta    *BrokerMetadata
}
