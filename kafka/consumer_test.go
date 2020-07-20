package kafka

import (
	"context"
	"testing"
	"time"
)

func TestConsumerStart(t *testing.T) {
	defaultCheckpoints, _ := NewPartitionCheckpoints([]string{"newest"}, []string{}, false)
	testCases := []struct {
		title                       string
		topicCheckpoints            map[string]*PartitionCheckpoints
		autoTopicCreation           bool
		exclusive                   bool
		idleTimeout                 time.Duration
		expectedErr                 string
		noTopicOnTheServer          bool
		forceTopicListFailure       bool
		forcePartitionsQueryFailure bool
	}{
		{
			title:       "empty topic list",
			expectedErr: "the topic list cannot be empty",
		},
		{
			title: "topic not found with auto topic creation disabled",
			topicCheckpoints: map[string]*PartitionCheckpoints{
				"topic": nil,
			},
			noTopicOnTheServer: true,
			autoTopicCreation:  false,
			expectedErr:        "failed to find the topic",
		},
		{
			title: "fail to fetch the topics from the server",
			topicCheckpoints: map[string]*PartitionCheckpoints{
				"topic": nil,
			},
			forceTopicListFailure: true,
			autoTopicCreation:     false,
			expectedErr:           "failed to fetch the topic list from the broker",
		},
		{
			title: "fail to fetch the partitions from the server",
			topicCheckpoints: map[string]*PartitionCheckpoints{
				"topic": nil,
			},
			forcePartitionsQueryFailure: true,
			expectedErr:                 "failed to fetch the partition offsets for topic",
		},
		{
			title: "start consuming with default checkpoints",
			topicCheckpoints: map[string]*PartitionCheckpoints{
				"topic": defaultCheckpoints,
			},
			expectedErr: "",
		},
	}

	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			store := newOffsetStoreMock()
			topics := make([]string, 0)
			for topic := range tC.topicCheckpoints {
				topics = append(topics, topic)
			}
			client := newClientMock(
				topics,
				8,
				tC.noTopicOnTheServer,
				tC.forceTopicListFailure,
				tC.forcePartitionsQueryFailure,
			)
			consumer := NewConsumer(store, client, &printerMock{}, tC.autoTopicCreation, tC.exclusive, tC.idleTimeout)
			timeout := tC.idleTimeout + (10 * time.Millisecond)
			ctx, cancel := context.WithTimeout(context.Background(), timeout)
			defer cancel()
			err := consumer.Start(ctx, tC.topicCheckpoints)
			if !checkError(err, tC.expectedErr) {
				t.Errorf("Expected start error: %q, Actual: %s", tC.expectedErr, err)
			}
			if tC.expectedErr != "" {
				return
			}
		})
	}
}
