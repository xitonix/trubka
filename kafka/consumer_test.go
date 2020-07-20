package kafka

import (
	"context"
	"sync"
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
		expectedOffsets             map[int32]int64
		publishToPartitions         []int32
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

			actualOffsets := make(map[int32]int64)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for event := range consumer.Events() {
					actualOffsets[event.Partition] = event.Offset
				}
			}()

			for _, topic := range topics {
				for _, partition := range tC.publishToPartitions {
					client.receive(topic, partition)
				}
			}

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
			wg.Wait()

			if len(tC.expectedOffsets) != len(actualOffsets) {
				t.Errorf("Expected Number of Received Messages: %d, Actual: %d", len(tC.expectedOffsets), len(actualOffsets))
			}

			for partition, expectedOffset := range tC.expectedOffsets {
				actual, ok := actualOffsets[partition]
				if !ok {
					t.Errorf("Did not receive a message with offset %d from partition %d", expectedOffset, partition)
				}
				if actual != expectedOffset {
					t.Errorf("Expected offset: %d from partition %d, Actual: %d", expectedOffset, partition, actual)
				}
			}
		})
	}
}
