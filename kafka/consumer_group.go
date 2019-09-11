package kafka

import (
	"regexp"

	"github.com/Shopify/sarama"
)

type GroupMember struct {
	ID         string
	ClientID   string
	ClientHost string
}

type GroupOffset struct {
	Latest  int64
	Current int64
}

func (g GroupOffset) Lag() int64 {
	if g.Latest > g.Current {
		return g.Latest - g.Current
	}
	return 0
}

type ConsumerGroup struct {
	Members           []GroupMember
	TopicGroupOffsets map[string]map[int32]GroupOffset
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
			ID:         id,
			ClientID:   m.ClientId,
			ClientHost: m.ClientHost,
		}
		c.Members = append(c.Members, member)
	}
}

type ConsumerGroups map[string]*ConsumerGroup
