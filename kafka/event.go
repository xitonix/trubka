package kafka

import "time"

// Event Kafka event.
type Event struct {
	// Topic the topic from which the message was consumed.
	Topic string
	// Key partition key.
	Key []byte
	// Value message content.
	Value []byte
	// Timestamp message timestamp.
	Timestamp time.Time
	// Partition the Kafka partition to which the message belong.
	Partition int32
	// Offset the message offset.
	Offset int64
}
