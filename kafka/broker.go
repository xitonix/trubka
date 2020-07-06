package kafka

import (
	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

// ControllerBrokerLabel controller node marker.
const ControllerBrokerLabel = "*"

// Broker represents a Kafka broker node.
type Broker struct {
	// Address the raw address of the broker.
	Address string `json:"-"`
	// Id the broker id returned from the server.
	Id int32 `json:"id"`
	// Host the printable broker address.
	Host string `json:"host"`
	// IsController is true if the broker is a controller node.
	IsController   bool `json:"controller"`
	*sarama.Broker `json:"-"`
}

// NewBroker creates a new instance of Kafka broker.
func NewBroker(broker *sarama.Broker, controllerId int32) *Broker {
	address := broker.Addr()
	id := broker.ID()
	return &Broker{
		Address:      address,
		Host:         internal.RemovePort(address),
		Id:           id,
		IsController: controllerId == id,
		Broker:       broker,
	}
}

// MarkedHostName returns the marked Host name if the broker is a controller, otherwise the original Host value will be returned.
func (b *Broker) MarkedHostName() string {
	if b.IsController {
		return b.Host + ControllerBrokerLabel
	}
	return b.Host
}

// BrokersById sorts the brokers by Id.
type BrokersById []*Broker

func (b BrokersById) Len() int {
	return len(b)
}

func (b BrokersById) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b BrokersById) Less(i, j int) bool {
	return b[i].Id < b[j].Id
}
