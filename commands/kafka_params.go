package commands

import (
	"crypto/tls"
	"io"

	"github.com/xitonix/trubka/internal"
	"github.com/xitonix/trubka/kafka"
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

func (p *KafkaParameters) createConsumer(prn internal.Printer, env string, autoTopic bool, logW io.Writer) (*kafka.Consumer, error) {
	brokers := getBrokers(p.Brokers)
	return kafka.NewConsumer(
		brokers, prn,
		env,
		autoTopic,
		kafka.WithClusterVersion(p.Version),
		kafka.WithTLS(p.TLS),
		kafka.WithLogWriter(logW),
		kafka.WithSASL(p.SASLMechanism,
			p.SASLUsername,
			p.SASLPassword))
}
