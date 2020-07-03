package kafka

import "github.com/Shopify/sarama"

// SASLHandshakeVersion SASL handshake version.
type SASLHandshakeVersion string

const (
	// SASLHandshakeV0 version 0
	SASLHandshakeV0 SASLHandshakeVersion = "v0"
	// SASLHandshakeV1 version 1
	SASLHandshakeV1 SASLHandshakeVersion = "v1"
)

func (s SASLHandshakeVersion) toSaramaVersion() int16 {
	switch s {
	case SASLHandshakeV0:
		return sarama.SASLHandshakeV0
	default:
		return sarama.SASLHandshakeV1
	}
}
