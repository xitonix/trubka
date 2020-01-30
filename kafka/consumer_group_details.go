package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type GroupMembers map[string]*GroupMemberDetails

type ConsumerGroupDetails struct {
	Name         string
	State        string
	Members      GroupMembers
	Protocol     string
	Coordinator  Broker
	ProtocolType string
}

type ConsumerGroupDetailsByName []*ConsumerGroupDetails

func (c ConsumerGroupDetailsByName) Len() int {
	return len(c)
}

func (c ConsumerGroupDetailsByName) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ConsumerGroupDetailsByName) Less(i, j int) bool {
	return c[i].Name < c[j].Name
}

func (c *ConsumerGroupDetails) String() string {
	return fmt.Sprintf("         Name: %s\n  Coordinator: %s\n        State: %s\n     Protocol: %s\nProtocol Type: %s",
		c.Name,
		c.Coordinator.Host,
		c.State,
		c.Protocol,
		c.ProtocolType)
}

type GroupMemberDetails struct {
	ClientHost      string
	TopicPartitions TopicPartitions
}

func fromGroupMemberDescription(md *sarama.GroupMemberDescription) (*GroupMemberDetails, error) {
	assignments, err := md.GetMemberAssignment()
	if err != nil {
		return nil, err
	}
	return &GroupMemberDetails{
		ClientHost:      md.ClientHost,
		TopicPartitions: assignments.Topics,
	}, nil
}
