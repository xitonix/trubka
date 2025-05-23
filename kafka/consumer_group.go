package kafka

import (
	"fmt"
)

// GroupMember represents a consumer group member.
type GroupMember struct {
	// ID the member identifier.
	ID string
	// ClientID client ID.
	ClientID string
	// Host the host name/IP of the client machine.
	Host string
}

func (g GroupMember) String() string {
	return fmt.Sprintf("%s [%s]", g.ID, g.Host)
}

// ConsumerGroup represents a consumer group.
type ConsumerGroup struct {
	// Members the clients attached to the consumer groups.
	Members []GroupMember
	// TopicOffsets the offsets of each topic belong to the group.
	TopicOffsets TopicPartitionOffset
	// Coordinator the coordinator of the consumer group
	Coordinator Broker
}

// ConsumerGroups the map of consumer groups keyed by consumer group ID.
type ConsumerGroups map[string]*ConsumerGroup

// Names returns the names of the consumer groups
func (c ConsumerGroups) Names() []string {
	names := make([]string, len(c))
	i := 0
	for name := range c {
		names[i] = name
		i++
	}
	return names
}
