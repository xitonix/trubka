package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

type Broker struct {
	Address      string
	ID           int32
	Host         string
	IsController bool
	*sarama.Broker
}

func NewBroker(broker *sarama.Broker, controllerId int32) *Broker {
	address := broker.Addr()
	id := broker.ID()
	return &Broker{
		Address:      address,
		Host:         internal.RemovePort(address),
		ID:           id,
		IsController: controllerId == id,
		Broker:       broker,
	}
}

func (b *Broker) String() string {
	var controller string
	if b.IsController {
		controller = " [C]"
	}
	return fmt.Sprintf("%d > %s%s", b.ID, b.Host, controller)
}

type BrokersById []*Broker

func (b BrokersById) Len() int {
	return len(b)
}
func (b BrokersById) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}
func (b BrokersById) Less(i, j int) bool {
	return b[i].ID < b[j].ID
}
