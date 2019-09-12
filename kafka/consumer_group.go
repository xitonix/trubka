package kafka

import (
	"fmt"
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

func (g GroupMember) String() string {
	return fmt.Sprintf("%s/%s(%s)", g.ID, g.ClientID, g.Host)
}

// ConsumerGroup represents a consumer group.
type ConsumerGroup struct {
	// Members the clients attached to the consumer groups.
	Members []GroupMember
	// TopicOffsets the offsets of each topic belong to the group.
	TopicOffsets TopicPartitionOffset
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

// ConsumerGroups the map of consumer groups keyed by consumer group ID.
type ConsumerGroups map[string]*ConsumerGroup
