package commands

import (
	"crypto/tls"
)

// KafkaParameters holds CLI parameters to connect to Kafka.
type KafkaParameters struct {
	// Brokers a comma separated list of host:port strings.
	Brokers string
	// Version the cluster version.
	Version string
	// TLS TLS settings.
	TLS *tls.Config
	// SASLMechanism SASL authentication mechanism.
	SASLMechanism string
	// SASLUsername SASL username.
	SASLUsername string
	// SASLPassword SASL password.
	SASLPassword string
	// SASLHandshakeVersion SASL handshake version.
	SASLHandshakeVersion string
}

// TLSParameters holds TLS connection parameters.
type TLSParameters struct {
	// Enabled true if TLS is requested by the user.
	Enabled bool
	// CACert the path to CA Cert file.
	CACert string
	// ClientCert path to client certification file to enable mutual TLS authentication.
	ClientCert string
	// ClientCert path to client private key file to enable mutual TLS authentication.
	//
	// If set, the client certificate file must also be provided.
	ClientKey string
}
