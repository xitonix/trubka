package kafka

import "fmt"

type Broker struct {
	Address string
	ID      int
	Meta    *BrokerMetadata
}

func (b Broker) String() string {
	return fmt.Sprintf("%s (ID: %d)", b.Address, b.ID)
}
