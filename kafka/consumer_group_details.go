package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
)

type GroupMembers map[string]*GroupMemberDetails

type ConsumerGroupDetails struct {
	Name         string       `json:"name"`
	State        string       `json:"state"`
	Protocol     string       `json:"protocol"`
	ProtocolType string       `json:"protocol_type"`
	Coordinator  Broker       `json:"coordinator"`
	Members      GroupMembers `json:"members,omitempty"`
}

func (c *ConsumerGroupDetails) ToJson(includeMembers bool) interface{} {
	if c == nil {
		return nil
	}
	type assignment struct {
		Topic      string  `json:"topic"`
		Partitions []int32 `json:"partitions"`
	}
	type member struct {
		Id          string        `json:"id"`
		Host        string        `json:"host"`
		Assignments []*assignment `json:"assignments"`
	}
	type protocol struct {
		Name string `json:"name,omitempty"`
		Type string `json:"type,omitempty"`
	}
	output := struct {
		Name        string    `json:"name"`
		State       string    `json:"state,omitempty"`
		Protocol    *protocol `json:"protocol,omitempty"`
		Coordinator *Broker   `json:"coordinator,omitempty"`
		Members     []*member `json:"members,omitempty"`
	}{
		Name:    c.Name,
		State:   c.State,
		Members: []*member{},
	}

	if len(c.Coordinator.Host) > 0 {
		output.Coordinator = &c.Coordinator
	}

	if len(c.Protocol) > 0 {
		output.Protocol = &protocol{
			Name: c.Protocol,
			Type: c.ProtocolType,
		}
	}

	if includeMembers {
		for id, gMember := range c.Members {
			m := &member{
				Id:          id,
				Host:        gMember.ClientHost,
				Assignments: []*assignment{},
			}
			for topic, partitions := range gMember.TopicPartitions {
				m.Assignments = append(m.Assignments, &assignment{
					Topic:      topic,
					Partitions: partitions,
				})
			}
			output.Members = append(output.Members, m)
		}
	}
	return output
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

type GroupMemberDetails struct {
	ClientHost      string          `json:"client_host"`
	TopicPartitions TopicPartitions `json:"topic_partitions"`
}

func fromGroupMemberDescription(md *sarama.GroupMemberDescription) (*GroupMemberDetails, error) {
	assignments, err := md.GetMemberAssignment()
	if err != nil {
		return nil, err
	}
	return &GroupMemberDetails{
		ClientHost:      strings.Trim(md.ClientHost, "/"),
		TopicPartitions: assignments.Topics,
	}, nil
}
