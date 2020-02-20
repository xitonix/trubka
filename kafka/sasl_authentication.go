package kafka

import (
	"crypto/sha256"
	"crypto/sha512"
	"hash"
	"strings"

	"github.com/Shopify/sarama"
)

const (
	SASLMechanismNone     = "none"
	SASLMechanismPlain    = "plain"
	SASLMechanismSCRAM256 = "scram-sha-256"
	SASLMechanismSCRAM512 = "scram-sha-512"
)

type sasl struct {
	mechanism sarama.SASLMechanism
	username  string
	password  string
	client    func() sarama.SCRAMClient
	version   int16
}

// This will return nil if the mechanism is not valid.
func newSASL(mechanism, username, password string, version SASLHandshakeVersion) *sasl {
	switch strings.ToLower(mechanism) {
	case SASLMechanismPlain:
		return &sasl{
			mechanism: sarama.SASLTypePlaintext,
			username:  username,
			password:  password,
			version:   version.toSaramaVersion(),
		}
	case SASLMechanismSCRAM256:
		hash := func() hash.Hash { return sha256.New() }
		return &sasl{
			client:    func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: hash} },
			mechanism: sarama.SASLTypeSCRAMSHA256,
			username:  username,
			password:  password,
			version:   version.toSaramaVersion(),
		}
	case SASLMechanismSCRAM512:
		hash := func() hash.Hash { return sha512.New() }
		return &sasl{
			client:    func() sarama.SCRAMClient { return &xdgSCRAMClient{HashGeneratorFcn: hash} },
			mechanism: sarama.SASLTypeSCRAMSHA512,
			username:  username,
			password:  password,
			version:   version.toSaramaVersion(),
		}
	default:
		return nil
	}
}
