package kafka

import (
	"fmt"
)

type Broker struct {
	Address string
	ID      int32
}

func (b Broker) String() string {
	return fmt.Sprintf("%d: %s", b.ID, b.Address)
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
