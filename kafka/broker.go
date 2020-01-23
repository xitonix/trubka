package kafka

import (
	"fmt"
	"strings"

	"github.com/Shopify/sarama"
)

type Broker struct {
	Address string
	ID      int32
	Host    string
}

func NewBroker(broker *sarama.Broker) Broker {
	address := broker.Addr()
	b := Broker{
		Address: address,
		Host:    address,
		ID:      broker.ID(),
	}
	if i := strings.Index(b.Address, ":"); i > 0 {
		b.Host = b.Address[:i]
	}
	return b
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
