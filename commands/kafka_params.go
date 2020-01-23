package commands

import (
	"crypto/tls"
)

type KafkaParameters struct {
	Brokers       string
	Version       string
	TLS           *tls.Config
	SASLMechanism string
	SASLUsername  string
	SASLPassword  string
}

type TLSParameters struct {
	Enabled    bool
	CACert     string
	ClientCert string
	ClientKey  string
}
