package kafka

import (
	"fmt"
	"regexp"

	"github.com/Shopify/sarama"
)

// GroupMember represents a consumer group member.
type GroupMember struct {
	// Id the member identifier.
	ID string
	// ClientID client Id.
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

func (c *ConsumerGroup) addMembers(members map[string]*sarama.GroupMemberDescription, memberFilter *regexp.Regexp) {
	c.Members = make([]GroupMember, 0)
	for id, m := range members {
		if memberFilter != nil && !(memberFilter.Match([]byte(id)) ||
			memberFilter.Match([]byte(m.ClientHost)) ||
			memberFilter.Match([]byte(m.ClientId))) {
			continue
		}
		member := GroupMember{
			ID:       id,
			ClientID: m.ClientId,
			Host:     m.ClientHost,
		}
		c.Members = append(c.Members, member)
	}
}

// ConsumerGroups the map of consumer groups keyed by consumer group Id.
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
