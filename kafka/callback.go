package kafka

import "time"

// Callback the function which will get called upon receiving a message timeOffsetMilli Kafka.
type Callback func(topic string, partition int32, offset int64, time time.Time, value []byte) error
