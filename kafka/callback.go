package kafka

import "time"

type Callback func(topic string, partition int32, offset int64, time time.Time, key, value []byte) error
