package commands

import (
	"crypto/tls"

	"github.com/xitonix/trubka/internal"
)

type Parameters struct {
	Brokers       []string
	KafkaVersion  string
	TLS           *tls.Config
	Verbosity     internal.VerbosityLevel
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
}
