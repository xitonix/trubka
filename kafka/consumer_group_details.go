package kafka

import "github.com/Shopify/sarama"

type ConsumerGroupDetails struct {
	State        string
	Members      map[string]*GroupMemberDetails
	Protocol     string
	Coordinator  Broker
	ProtocolType string
}

type GroupMemberDetails struct {
	ClientHost      string
	TopicPartitions map[string][]int32
}

func fromSaramaGroupMemberDescription(md *sarama.GroupMemberDescription) (*GroupMemberDetails, error) {
	assignments, err := md.GetMemberAssignment()
	if err != nil {
		return nil, err
	}
	return &GroupMemberDetails{
		ClientHost:      md.ClientHost,
		TopicPartitions: assignments.Topics,
	}, nil
}
