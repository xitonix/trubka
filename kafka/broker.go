package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

type Broker struct {
	Address string
	ID      int32
	Host    string
}

func NewBroker(broker *sarama.Broker) Broker {
	address := broker.Addr()
	return Broker{
		Address: address,
		Host:    internal.RemovePort(address),
		ID:      broker.ID(),
	}
}

func (b Broker) String() string {
	return fmt.Sprintf("%d: %s", b.ID, b.Host)
}

type BrokersById []Broker

func (b BrokersById) Len() int {
	return len(b)
}
func (b BrokersById) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
func (b BrokersById) Less(i, j int) bool {
	return b[i].ID < b[j].ID
}
