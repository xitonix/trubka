package kafka

type shutdownReason int8

const (
	cancelledByUser shutdownReason = iota
	noMoreMessage
	reachedStopCheckpoint
)

var (
	shutdownReasonToString = map[shutdownReason]string{
		cancelledByUser:       "Cancelled by user",
		noMoreMessage:         "No more message received",
		reachedStopCheckpoint: "Reached stop checkpoint",
	}
)

func (s shutdownReason) String() string {
	return shutdownReasonToString[s]
}
