package kafka

import (
	"regexp"

	"github.com/Shopify/sarama"
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

// GroupOffset represents a consumer group partition offset.
type GroupOffset struct {
	// Latest the latest available offset of the partition.
	Latest int64
	// Current the current value of consumer group offset.
	Current int64
}

// Lag returns the lag of the consumer group offset.
func (g GroupOffset) Lag() int64 {
	if g.Latest > g.Current {
		return g.Latest - g.Current
	}
	return 0
}

// ConsumerGroup represents a consumer group.
type ConsumerGroup struct {
	// Members the clients attached to the consumer groups.
	Members []GroupMember
	// TopicOffsets the offsets of each topic belong to the group.
	TopicOffsets map[string]map[int32]GroupOffset
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

// ConsumerGroups the map of consumer groups.
type ConsumerGroups map[string]*ConsumerGroup
