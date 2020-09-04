package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

// ControllerBrokerLabel controller node marker.
const ControllerBrokerLabel = "*"

// Broker represents a Kafka broker node.
type Broker struct {
	// Address the raw address of the broker.
	Address string `json:"-"`
	// ID the broker id returned from the server.
	ID int32 `json:"id"`
	// Host the printable broker address.
	Host string `json:"host"`
	// IsController is true if the broker is a controller node.
	IsController   bool `json:"controller"`
	*sarama.Broker `json:"-"`
}

// NewBroker creates a new instance of Kafka broker.
func NewBroker(broker *sarama.Broker, controllerID int32) *Broker {
	address := broker.Addr()
	id := broker.ID()
	return &Broker{
		Address:      address,
		Host:         internal.RemovePort(address),
		ID:           id,
		IsController: controllerID == id,
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

// String returns the string representation of the broker.
func (b *Broker) String() string {
	if b == nil {
		return ""
	}
	return fmt.Sprintf("%d/%s", b.ID, b.Host)
}

// BrokersByID sorts the brokers by ID.
type BrokersByID []*Broker

func (b BrokersByID) Len() int {
	return len(b)
}

func (b BrokersByID) Swap(i, j int) {
	b[i], b[j] = b[j], b[i]
}

func (b BrokersByID) Less(i, j int) bool {
	return b[i].ID < b[j].ID
}
