package kafka

import (
	"context"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/Shopify/sarama"
)

type _checkpointInput struct {
	from      []string
	to        []string
	exclusive bool
}

type _consumedOffset struct {
	min int64
	max int64
}

func TestConsumerStart(t *testing.T) {
	testCases := []struct {
		title                       string
		autoTopicCreation           bool
		expectedErr                 string
		noTopicOnTheServer          bool
		forceTopicListFailure       bool
		forcePartitionsQueryFailure bool
		checkpoints                 _checkpointInput
		topics                      []string
	}{
		{
			title:       "empty topic list",
			expectedErr: "the topic list cannot be empty",
			checkpoints: _checkpointInput{from: []string{"newest"}},
		},
		{
			title:              "topic not found with auto topic creation disabled",
			noTopicOnTheServer: true,
			autoTopicCreation:  false,
			expectedErr:        "failed to find the topic",
			checkpoints:        _checkpointInput{from: []string{"newest"}},
			topics:             []string{"topic"},
		},
		{
			title:                 "fail to fetch the topics from the server",
			forceTopicListFailure: true,
			autoTopicCreation:     false,
			expectedErr:           "failed to fetch the topic list from the broker",
			checkpoints:           _checkpointInput{from: []string{"newest"}},
			topics:                []string{"topic"},
		},
		{
			title:                       "fail to fetch the partitions from the server",
			forcePartitionsQueryFailure: true,
			expectedErr:                 "failed to fetch the partition offsets for topic",
			checkpoints:                 _checkpointInput{from: []string{"newest"}},
			topics:                      []string{"topic"},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			store := newOffsetStoreMock(false)
			pc, err := NewPartitionCheckpoints(tC.checkpoints.from, tC.checkpoints.to, tC.checkpoints.exclusive)
			if err != nil {
				t.Fatalf("Did not expect an error, but received: '%v'", err)
			}

			tpc := make(map[string]*PartitionCheckpoints)
			for _, topic := range tC.topics {
				tpc[topic] = pc
			}

			client := newClientMock(
				tC.topics,
				8,
				8,
				tC.noTopicOnTheServer,
				tC.forceTopicListFailure,
				tC.forcePartitionsQueryFailure,
				false,
			)

			consumer := NewConsumer(store, client, &printerMock{}, tC.autoTopicCreation, tC.checkpoints.exclusive, 0)
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
			defer cancel()

			err = consumer.Start(ctx, tpc)
			if !checkError(err, tC.expectedErr) {
				t.Errorf("Expected start error: %q, Actual: %v", tC.expectedErr, err)
			}
		})
	}
}

func TestConsumerTimestampCheckpoints(t *testing.T) {
	testCases := []struct {
		title                    string
		idleTimeout              time.Duration
		checkpoints              _checkpointInput
		topics                   []string
		numberOfActivePartitions int
		availableOffsets         map[int32]map[interface{}]int64
		expectedOffsets          map[int32]_consumedOffset
		messages                 map[int32][]string
		forceOffsetQueryFailure  bool
		expectedErr              string
	}{
		{

			title: "offset query failure",
			checkpoints: _checkpointInput{
				from:      []string{"2020-08-20T10:20:00"},
				to:        []string{"2020-08-20T10:22:00"},
				exclusive: false,
			},
			forceOffsetQueryFailure: true,
			expectedErr:             "failed to retrieve the time-based offset",
			topics:                  []string{"topic"},
		},
		{
			title: "global checkpoints in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"2020-08-20T10:20:00"},
				to:        []string{"2020-08-20T10:22:00"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			availableOffsets: map[int32]map[interface{}]int64{
				0: {
					"2020-08-20T10:20:00": 3,
				},
				1: {
					"2020-08-20T10:20:00": 5,
				},
			},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 3,
					max: 5,
				},
				1: {
					min: 5,
					max: 7,
				},
			},
		},
		{
			title: "global checkpoints in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"2020-08-20T10:20:00"},
				to:        []string{"2020-08-20T10:22:00"},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			availableOffsets: map[int32]map[interface{}]int64{
				0: {
					"2020-08-20T10:20:00": 3,
				},
				1: {
					"2020-08-20T10:20:00": 5,
				},
			},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 3,
					max: 5,
				},
				1: {
					min: 5,
					max: 7,
				},
			},
		},
		{
			title: "partition specific checkpoints in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"0#2020-08-20T10:20:00"},
				to:        []string{"0#2020-08-20T10:22:00"},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
				}},
			numberOfActivePartitions: 1,
			availableOffsets: map[int32]map[interface{}]int64{
				0: {
					"2020-08-20T10:20:00": 3,
				},
				1: {
					"2020-08-20T10:20:00": 3,
				},
			},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 3,
					max: 5,
				},
			},
		},
		{
			title: "partition specific checkpoints in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"0#2020-08-20T10:20:00"},
				to:        []string{"0#2020-08-20T10:22:00"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
					"2020-08-20T10:24:00",
				}},
			availableOffsets: map[int32]map[interface{}]int64{
				0: {
					"2020-08-20T10:20:00": 3,
				},
				1: {
					sarama.OffsetNewest: 5,
				},
			},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 3,
					max: 5,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 4,
				},
			},
		},
		{
			title:       "idle timeout with global checkpoints",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"2020-08-20T10:20:00"},
				to:        []string{"2020-08-20T10:22:00"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			availableOffsets: map[int32]map[interface{}]int64{
				0: {
					"2020-08-20T10:20:00": 3,
				},
				1: {
					"2020-08-20T10:20:00": 5,
				},
			},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 3,
					max: 5,
				},
				1: {
					min: 5,
					max: 7,
				},
			},
		},
		{
			title:       "idle timeout with partition specific checkpoints",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"0#2020-08-20T10:20:00"},
				to:        []string{"0#2020-08-20T10:22:00"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
					"2020-08-20T10:24:00",
				}},
			availableOffsets: map[int32]map[interface{}]int64{
				0: {
					"2020-08-20T10:20:00": 3,
				},
				1: {
					sarama.OffsetNewest: 5,
				},
			},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 3,
					max: 5,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 4,
				},
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			store := newOffsetStoreMock(false)
			pc, err := NewPartitionCheckpoints(tC.checkpoints.from, tC.checkpoints.to, tC.checkpoints.exclusive)
			if err != nil {
				t.Fatalf("Did not expect an error, but received: '%v'", err)
			}

			tpc := make(map[string]*PartitionCheckpoints)
			for _, topic := range tC.topics {
				tpc[topic] = pc
			}

			client := newClientMock(
				tC.topics,
				8,
				tC.numberOfActivePartitions,
				false,
				false,
				false,
				tC.forceOffsetQueryFailure)

			for partition, atOffset := range tC.availableOffsets {
				for at, offset := range atOffset {
					client.setAvailableOffset(partition, at, offset)
				}
			}

			consumer := NewConsumer(store, client, &printerMock{}, true, tC.checkpoints.exclusive, tC.idleTimeout)

			actualOffsets := make(map[int32]*_consumedOffset)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for event := range consumer.Events() {
					offsets, ok := actualOffsets[event.Partition]
					if ok {
						offsets.max = event.Offset
					} else {
						actualOffsets[event.Partition] = &_consumedOffset{
							min: event.Offset,
							max: event.Offset,
						}
					}
				}
			}()

			go func() {
				<-client.ready
				for _, topic := range tC.topics {
					for partition, dates := range tC.messages {
						for _, at := range dates {
							client.receive(topic, partition, at)
						}
					}
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var autoShutdown int32

			timeout := 10 * time.Millisecond
			if tC.idleTimeout > 0 {
				timeout = 1100 * time.Millisecond
			}

			go func() {
				<-client.ready
				<-time.After(timeout)
				atomic.StoreInt32(&autoShutdown, 1)
				cancel()
			}()

			err = consumer.Start(ctx, tpc)
			if !checkError(err, tC.expectedErr) {
				t.Errorf("Expected start error: %q, Actual: %v", tC.expectedErr, err)
			}

			if tC.expectedErr != "" {
				return
			}

			wg.Wait()

			if tC.idleTimeout > 0 && atomic.LoadInt32(&autoShutdown) > 0 {
				t.Errorf("The consumer was supposed to be stopped because of the idle timeout, but it did not happen.")
			}

			if len(tC.expectedOffsets) != len(actualOffsets) {
				t.Errorf("Expected Number of Partitions with Message: %d, Actual: %d", len(tC.expectedOffsets), len(actualOffsets))
			}

			for partition, expectedOffset := range tC.expectedOffsets {
				actual, ok := actualOffsets[partition]
				if !ok {
					t.Fatalf("Have not received any message from partition %d", partition)
				}
				if actual.min != expectedOffset.min {
					t.Errorf("Expected minimum offset: %d from partition %d, Actual: %d", expectedOffset.min, partition, actual.min)
				}
				if actual.max != expectedOffset.max {
					t.Errorf("Expected maximum offset: %d from partition %d, Actual: %d", expectedOffset.max, partition, actual.max)
				}
			}
		})
	}
}

func TestConsumerExplicitCheckpoints(t *testing.T) {
	testCases := []struct {
		title                    string
		idleTimeout              time.Duration
		checkpoints              _checkpointInput
		topics                   []string
		numberOfActivePartitions int
		availableOffsets         map[int32]map[interface{}]int64
		expectedOffsets          map[int32]_consumedOffset
		messages                 map[int32][]string
		forceOffsetQueryFailure  bool
		expectedErr              string
	}{
		{
			title:                   "offset query failure",
			forceOffsetQueryFailure: true,
			expectedErr:             "failed to retrieve the current offset value",
			checkpoints: _checkpointInput{
				from:      []string{"1"},
				to:        []string{"2"},
				exclusive: false,
			},
			topics: []string{"topic"},
		},
		{
			title: "global checkpoints in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"1"},
				to:        []string{"2"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
				1: {
					min: 1,
					max: 2,
				},
			},
		},
		{
			title: "global checkpoints in inclusive mode with start offset greater than available offset",
			checkpoints: _checkpointInput{
				from:      []string{strconv.Itoa(int(_endOfStream + 1))},
				to:        []string{strconv.Itoa(int(_endOfStream + 2))},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{},
		},
		{
			title: "global checkpoints in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"1"},
				to:        []string{"2"},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
				1: {
					min: 1,
					max: 2,
				},
			},
		},
		{
			title: "global checkpoints in exclusive mode with start offset greater than available offset",
			checkpoints: _checkpointInput{
				from:      []string{strconv.Itoa(int(_endOfStream + 1))},
				to:        []string{strconv.Itoa(int(_endOfStream + 2))},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{},
		},
		{
			title: "partition specific checkpoints in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"0#1"},
				to:        []string{"0#2"},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
				}},
			numberOfActivePartitions: 1,
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
			},
		},
		{
			title: "partition specific checkpoints in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"0#1"},
				to:        []string{"0#2"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
					"2020-08-20T10:24:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 4,
				},
			},
		},
		{
			title: "partition specific checkpoints in exclusive mode with start offset greater than available offset",
			checkpoints: _checkpointInput{
				from:      []string{fmt.Sprintf("0#%d", _endOfStream+1)},
				to:        []string{fmt.Sprintf("0#%d", _endOfStream+2)},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
				}},
			numberOfActivePartitions: 1,
			expectedOffsets:          map[int32]_consumedOffset{},
		},
		{
			title: "partition specific checkpoints in inclusive mode with start offset greater than available offset",
			checkpoints: _checkpointInput{
				from:      []string{fmt.Sprintf("0#%d", _endOfStream+1)},
				to:        []string{fmt.Sprintf("0#%d", _endOfStream+2)},
				exclusive: false,
			},
			topics:                   []string{"topic"},
			numberOfActivePartitions: 7,
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
					"2020-08-20T10:24:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				1: {
					min: _endOfStream,
					max: _endOfStream + 4,
				},
			},
		},
		{
			title:       "idle timeout with global checkpoints",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"1"},
				to:        []string{"2"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
				1: {
					min: 1,
					max: 2,
				},
			},
		},
		{
			title:       "idle timeout with partition specific checkpoints in inclusive mode",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"0#1"},
				to:        []string{"0#2"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
					"2020-08-20T10:24:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 4,
				},
			},
		},
		{
			title:       "idle timeout with partition specific checkpoints in exclusive mode",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"0#1"},
				to:        []string{"0#2"},
				exclusive: true,
			},
			topics:                   []string{"topic"},
			numberOfActivePartitions: 1,
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
					"2020-08-20T10:24:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 1,
					max: 2,
				},
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			store := newOffsetStoreMock(false)
			pc, err := NewPartitionCheckpoints(tC.checkpoints.from, tC.checkpoints.to, tC.checkpoints.exclusive)
			if err != nil {
				t.Fatalf("Did not expect an error, but received: '%v'", err)
			}

			tpc := make(map[string]*PartitionCheckpoints)
			for _, topic := range tC.topics {
				tpc[topic] = pc
			}

			client := newClientMock(
				tC.topics,
				8,
				tC.numberOfActivePartitions,
				false,
				false,
				false,
				tC.forceOffsetQueryFailure)

			for partition, atOffset := range tC.availableOffsets {
				for at, offset := range atOffset {
					client.setAvailableOffset(partition, at, offset)
				}
			}

			consumer := NewConsumer(store, client, &printerMock{}, true, tC.checkpoints.exclusive, tC.idleTimeout)

			actualOffsets := make(map[int32]*_consumedOffset)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for event := range consumer.Events() {
					offsets, ok := actualOffsets[event.Partition]
					if ok {
						offsets.max = event.Offset
					} else {
						actualOffsets[event.Partition] = &_consumedOffset{
							min: event.Offset,
							max: event.Offset,
						}
					}
				}
			}()

			go func() {
				<-client.ready
				for _, topic := range tC.topics {
					for partition, dates := range tC.messages {
						for _, at := range dates {
							client.receive(topic, partition, at)
						}
					}
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var autoShutdown int32

			timeout := 10 * time.Millisecond
			if tC.idleTimeout > 0 {
				timeout = 1100 * time.Millisecond
			}

			go func() {
				<-client.ready
				<-time.After(timeout)
				atomic.StoreInt32(&autoShutdown, 1)
				cancel()
			}()

			err = consumer.Start(ctx, tpc)
			if !checkError(err, tC.expectedErr) {
				t.Errorf("Expected start error: %q, Actual: %v", tC.expectedErr, err)
			}

			if tC.expectedErr != "" {
				return
			}

			wg.Wait()

			if tC.idleTimeout > 0 && atomic.LoadInt32(&autoShutdown) > 0 {
				t.Errorf("The consumer was supposed to be stopped because of the idle timeout, but it did not happen.")
			}

			if len(tC.expectedOffsets) != len(actualOffsets) {
				t.Errorf("Expected Number of Partitions with Message: %d, Actual: %d", len(tC.expectedOffsets), len(actualOffsets))
			}

			for partition, expectedOffset := range tC.expectedOffsets {
				actual, ok := actualOffsets[partition]
				if !ok {
					t.Fatalf("Have not received any message from partition %d", partition)
				}
				if actual.min != expectedOffset.min {
					t.Errorf("Expected minimum offset: %d from partition %d, Actual: %d", expectedOffset.min, partition, actual.min)
				}
				if actual.max != expectedOffset.max {
					t.Errorf("Expected maximum offset: %d from partition %d, Actual: %d", expectedOffset.max, partition, actual.max)
				}
			}
		})
	}
}

func TestConsumerPredefinedCheckpoints(t *testing.T) {
	testCases := []struct {
		title                    string
		idleTimeout              time.Duration
		checkpoints              _checkpointInput
		topics                   []string
		numberOfActivePartitions int
		availableOffsets         map[int32]map[interface{}]int64
		expectedOffsets          map[int32]_consumedOffset
		messages                 map[int32][]string
	}{
		{
			title: "newest to newest plus two",
			checkpoints: _checkpointInput{
				from:      []string{"newest"},
				to:        []string{fmt.Sprintf("%d", _endOfStream+2)},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
			},
		},
		{
			title: "oldest plus two",
			checkpoints: _checkpointInput{
				from:      []string{"oldest"},
				to:        []string{"2"},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 0,
					max: 2,
				},
				1: {
					min: 0,
					max: 2,
				},
			},
		},
		{
			title: "newest to newest plus two in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"newest"},
				to:        []string{fmt.Sprintf("%d", _endOfStream+2)},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
			},
		},
		{
			title: "oldest plus two in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"oldest"},
				to:        []string{"2"},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 0,
					max: 2,
				},
				1: {
					min: 0,
					max: 2,
				},
			},
		},
		{
			title: "partition specific checkpoints in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"0#oldest", "1#newest"},
				to:        []string{"0#2", fmt.Sprintf("1#%d", _endOfStream+2)},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				2: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			numberOfActivePartitions: 2,
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 0,
					max: 2,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
			},
		},
		{
			title: "partition specific checkpoints in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"0#oldest", "1#newest"},
				to:        []string{"0#2", fmt.Sprintf("1#%d", _endOfStream+2)},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				2: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 0,
					max: 2,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
				2: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
			},
		},
		{
			title:       "idle timeout with newest global checkpoints",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"newest"},
				to:        []string{},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
			},
		},
		{
			title:       "idle timeout with oldest global checkpoints",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"oldest"},
				to:        []string{},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 0,
					max: 1,
				},
				1: {
					min: 0,
					max: 1,
				},
			},
		},
		{
			title:       "idle timeout with partition specific global checkpoints in exclusive mode",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"0#oldest"},
				to:        []string{},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			numberOfActivePartitions: 1,
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 0,
					max: 1,
				},
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			store := newOffsetStoreMock(false)
			pc, err := NewPartitionCheckpoints(tC.checkpoints.from, tC.checkpoints.to, tC.checkpoints.exclusive)
			if err != nil {
				t.Fatalf("Did not expect an error, but received: '%v'", err)
			}

			tpc := make(map[string]*PartitionCheckpoints)
			for _, topic := range tC.topics {
				tpc[topic] = pc
			}

			client := newClientMock(
				tC.topics,
				8,
				tC.numberOfActivePartitions,
				false,
				false,
				false,
				false)

			for partition, atOffset := range tC.availableOffsets {
				for at, offset := range atOffset {
					client.setAvailableOffset(partition, at, offset)
				}
			}

			consumer := NewConsumer(store, client, &printerMock{}, true, tC.checkpoints.exclusive, tC.idleTimeout)

			actualOffsets := make(map[int32]*_consumedOffset)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for event := range consumer.Events() {
					offsets, ok := actualOffsets[event.Partition]
					if ok {
						offsets.max = event.Offset
					} else {
						actualOffsets[event.Partition] = &_consumedOffset{
							min: event.Offset,
							max: event.Offset,
						}
					}
				}
			}()

			go func() {
				<-client.ready
				for _, topic := range tC.topics {
					for partition, dates := range tC.messages {
						for _, at := range dates {
							client.receive(topic, partition, at)
						}
					}
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var autoShutdown int32

			timeout := 10 * time.Millisecond
			if tC.idleTimeout > 0 {
				timeout = 1100 * time.Millisecond
			}

			go func() {
				<-client.ready
				<-time.After(timeout)
				atomic.StoreInt32(&autoShutdown, 1)
				cancel()
			}()

			err = consumer.Start(ctx, tpc)

			if err != nil {
				t.Fatalf("Did not expect any errors, but received %v", err)
			}

			wg.Wait()

			if tC.idleTimeout > 0 && atomic.LoadInt32(&autoShutdown) > 0 {
				t.Errorf("The consumer was supposed to be stopped because of the idle timeout, but it did not happen.")
			}

			if len(tC.expectedOffsets) != len(actualOffsets) {
				t.Errorf("Expected Number of Partitions with Message: %d, Actual: %d", len(tC.expectedOffsets), len(actualOffsets))
			}

			for partition, expectedOffset := range tC.expectedOffsets {
				actual, ok := actualOffsets[partition]
				if !ok {
					t.Fatalf("Have not received any message from partition %d", partition)
				}
				if actual.min != expectedOffset.min {
					t.Errorf("Expected minimum offset: %d from partition %d, Actual: %d", expectedOffset.min, partition, actual.min)
				}
				if actual.max != expectedOffset.max {
					t.Errorf("Expected maximum offset: %d from partition %d, Actual: %d", expectedOffset.max, partition, actual.max)
				}
			}
		})
	}
}

func TestConsumerLocalCheckpoints(t *testing.T) {
	testCases := []struct {
		title                    string
		idleTimeout              time.Duration
		checkpoints              _checkpointInput
		topics                   []string
		numberOfActivePartitions int
		availableOffsets         map[int32]map[interface{}]int64
		expectedOffsets          map[int32]_consumedOffset
		messages                 map[int32][]string
		localOffsets             map[int32]int64
		expectedErr              string
		forceOffsetQueryFailure  bool
		forceReadFailure         bool
	}{
		{
			title:                   "offset query failure",
			forceOffsetQueryFailure: true,
			expectedErr:             "failed to retrieve the current offset value",
			checkpoints: _checkpointInput{
				from:      []string{"local"},
				exclusive: false,
			},
			topics: []string{"topic"},
		},
		{
			title:            "local offset query failure",
			forceReadFailure: true,
			expectedErr:      "failed to query the local offset store",
			checkpoints: _checkpointInput{
				from:      []string{"local"},
				exclusive: false,
			},
			topics: []string{"topic"},
		},
		{
			title: "global checkpoint when no local offset is stored in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"local"},
				to:        []string{fmt.Sprintf("%d", _endOfStream+2)},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
			},
		},
		{
			title: "global checkpoint when no local offset is stored in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"local"},
				to:        []string{fmt.Sprintf("%d", _endOfStream+2)},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
			},
		},
		{
			title: "partition specific checkpoint when no local offset is stored in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"oldest", "0#local"},
				to:        []string{"2", fmt.Sprintf("0#%d", _endOfStream+2)},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
				1: {
					min: 0,
					max: 1,
				},
			},
		},
		{
			title: "partition specific checkpoint when no local offset is stored in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"oldest", "0#local"},
				to:        []string{"2", fmt.Sprintf("0#%d", _endOfStream+2)},
				exclusive: true,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
					"2020-08-20T10:22:00",
					"2020-08-20T10:23:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			numberOfActivePartitions: 1,
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 2,
				},
			},
		},


		{
			title: "partition specific checkpoint with local offsets in inclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"oldest", "0#local"},
				to:        []string{"2", "0#20"},
				exclusive: false,
			},
			localOffsets: map[int32]int64{
				0: 10,
				1: 5,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 10,
					max: 11,
				},
				1: {
					min: 0,
					max: 1,
				},
			},
		},
		{
			title: "partition specific checkpoint with local offsets in exclusive mode",
			checkpoints: _checkpointInput{
				from:      []string{"oldest", "0#local"},
				to:        []string{"2", "0#20"},
				exclusive: true,
			},
			localOffsets: map[int32]int64{
				0: 10,
				1: 5,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			numberOfActivePartitions: 1,
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 10,
					max: 11,
				},
			},
		},
		{
			title:       "idle timeout with no local offsets stored",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"local"},
				to:        []string{},
				exclusive: false,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
				1: {
					min: _endOfStream,
					max: _endOfStream + 1,
				},
			},
		},
		{
			title:       "idle timeout with local offsets",
			idleTimeout: 10 * time.Millisecond,
			checkpoints: _checkpointInput{
				from:      []string{"local"},
				to:        []string{},
				exclusive: false,
			},
			localOffsets: map[int32]int64{
				0: 10,
				1: 5,
			},
			topics: []string{"topic"},
			messages: map[int32][]string{
				0: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				},
				1: {
					"2020-08-20T10:20:00",
					"2020-08-20T10:21:00",
				}},
			expectedOffsets: map[int32]_consumedOffset{
				0: {
					min: 10,
					max: 11,
				},
				1: {
					min: 5,
					max: 6,
				},
			},
		},
	}

	for _, tC := range testCases {
		t.Run(tC.title, func(t *testing.T) {
			store := newOffsetStoreMock(tC.forceReadFailure)
			pc, err := NewPartitionCheckpoints(tC.checkpoints.from, tC.checkpoints.to, tC.checkpoints.exclusive)
			if err != nil {
				t.Fatalf("Did not expect an error, but received: '%v'", err)
			}

			tpc := make(map[string]*PartitionCheckpoints)
			for _, topic := range tC.topics {
				tpc[topic] = pc
				store.set(topic, tC.localOffsets)
			}

			client := newClientMock(
				tC.topics,
				8,
				tC.numberOfActivePartitions,
				false,
				false,
				false,
				tC.forceOffsetQueryFailure)

			for partition, atOffset := range tC.availableOffsets {
				for at, offset := range atOffset {
					client.setAvailableOffset(partition, at, offset)
				}
			}

			consumer := NewConsumer(store, client, &printerMock{}, true, tC.checkpoints.exclusive, tC.idleTimeout)

			actualOffsets := make(map[int32]*_consumedOffset)
			wg := sync.WaitGroup{}
			wg.Add(1)
			go func() {
				defer wg.Done()
				for event := range consumer.Events() {
					offsets, ok := actualOffsets[event.Partition]
					if ok {
						offsets.max = event.Offset
					} else {
						actualOffsets[event.Partition] = &_consumedOffset{
							min: event.Offset,
							max: event.Offset,
						}
					}
				}
			}()

			go func() {
				<-client.ready
				for _, topic := range tC.topics {
					for partition, dates := range tC.messages {
						for _, at := range dates {
							client.receive(topic, partition, at)
						}
					}
				}
			}()

			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()

			var autoShutdown int32

			timeout := 10 * time.Millisecond
			if tC.idleTimeout > 0 {
				timeout = 1100 * time.Millisecond
			}

			go func() {
				<-client.ready
				<-time.After(timeout)
				atomic.StoreInt32(&autoShutdown, 1)
				cancel()
			}()

			err = consumer.Start(ctx, tpc)

			if !checkError(err, tC.expectedErr) {
				t.Errorf("Expected start error: %q, Actual: %v", tC.expectedErr, err)
			}

			if tC.expectedErr != "" && len(tC.expectedOffsets) == 0 {
				return
			}

			wg.Wait()

			if tC.idleTimeout > 0 && atomic.LoadInt32(&autoShutdown) > 0 {
				t.Errorf("The consumer was supposed to be stopped because of the idle timeout, but it did not happen.")
			}

			if len(tC.expectedOffsets) != len(actualOffsets) {
				t.Errorf("Expected Number of Partitions with Message: %d, Actual: %d", len(tC.expectedOffsets), len(actualOffsets))
			}

			for partition, expectedOffset := range tC.expectedOffsets {
				actual, ok := actualOffsets[partition]
				if !ok {
					t.Fatalf("Have not received any message from partition %d", partition)
				}
				if actual.min != expectedOffset.min {
					t.Errorf("Expected minimum offset: %d from partition %d, Actual: %d", expectedOffset.min, partition, actual.min)
				}
				if actual.max != expectedOffset.max {
					t.Errorf("Expected maximum offset: %d from partition %d, Actual: %d", expectedOffset.max, partition, actual.max)
				}
			}
		})
	}
}
