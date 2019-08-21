package commands

import "github.com/xitonix/trubka/internal"

type Parameters struct {
	Brokers       []string
	KafkaVersion  string
	LogFile       string
	SASLUsername  string
	SASLPassword  string
	SASLMechanism string
	TLS           bool
	CACert        string
	ClientCert    string
	ClientKey     string
	Theme         string
	Verbosity     internal.VerbosityLevel
}
