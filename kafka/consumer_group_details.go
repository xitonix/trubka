package kafka

import (
	"fmt"

	"github.com/Shopify/sarama"
)

type GroupMembers map[string]*GroupMemberDetails

type ConsumerGroupDetails struct {
	State        string
	Members      GroupMembers
	Protocol     string
	Coordinator  Broker
	ProtocolType string
}

func (c *ConsumerGroupDetails) String() string {
	return fmt.Sprintf("  Coordinator: %s\n        State: %s\n     Protocol: %s\nProtocol Type: %s",
		c.Coordinator.Address,
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
