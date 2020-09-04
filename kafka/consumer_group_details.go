package kafka

import (
	"strings"

	"github.com/Shopify/sarama"
)

// GroupMembers represents a map to hold consumer group members.
type GroupMembers map[string]*GroupMemberDetails

// ConsumerGroupDetails represents consumer group details.
type ConsumerGroupDetails struct {
	// Name group name.
	Name string `json:"name"`
	// State group state.
	State string `json:"state"`
	// Protocol group protocol.
	Protocol string `json:"protocol"`
	// ProtocolType group protocol type.
	ProtocolType string `json:"protocol_type"`
	// Broker group coordinator.
	Coordinator Broker `json:"coordinator"`
	// Members consumer group members.
	Members GroupMembers `json:"members,omitempty"`
}

// ToJSON returns an object ready to be serialised into json string.
func (c *ConsumerGroupDetails) ToJSON(includeMembers bool) interface{} {
	if c == nil {
		return nil
	}
	type assignment struct {
		Topic      string  `json:"topic"`
		Partitions []int32 `json:"partitions"`
	}
	type member struct {
		ID          string        `json:"id"`
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
				ID:          id,
				Host:        gMember.ClientHost,
				Assignments: []*assignment{},
			}
			for topic, partitions := range gMember.Assignments {
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

// ConsumerGroupDetailsByName sorts a list of consumer group details by group name.
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

// GroupMemberDetails represents consumer group member details.
type GroupMemberDetails struct {
	// ClientHost the host name of the group member.
	ClientHost string `json:"client_host"`
	// Assignments partitions assigned to the group member for each topic.
	Assignments TopicPartitions `json:"assignments"`
}

func fromGroupMemberDescription(md *sarama.GroupMemberDescription) (*GroupMemberDetails, error) {
	assignments, err := md.GetMemberAssignment()
	if err != nil {
		return nil, err
	}
	return &GroupMemberDetails{
		ClientHost:  strings.Trim(md.ClientHost, "/"),
		Assignments: assignments.Topics,
	}, nil
}
