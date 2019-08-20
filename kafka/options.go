package kafka

import (
	"crypto/tls"
	"io"
	"io/ioutil"

	"github.com/Shopify/sarama"

	"github.com/xitonix/trubka/internal"
)

var (
	DefaultClusterVersion = sarama.MaxVersion.String()
)

// Options holds the configuration settings for kafka consumer.
type Options struct {
	// DisableErrorReporting disables sending consumer errors to the Errors() channel.
	DisableErrorReporting bool
	// ClusterVersion kafka cluster version.
	ClusterVersion string
	// TLS configuration to connect to Kafka cluster.
	TLS       *tls.Config
	sasl      *sasl
	logWriter io.Writer
}

// NewOptions creates a new Options object with default values.
func NewOptions() *Options {
	return &Options{
		DisableErrorReporting: false,
		ClusterVersion:        DefaultClusterVersion,
		logWriter:             ioutil.Discard,
	}
}

// Option represents a configuration function.
type Option func(options *Options)

// WithClusterVersion kafka cluster version.
func WithClusterVersion(version string) Option {
	return func(options *Options) {
		if internal.IsEmpty(version) {
			version = DefaultClusterVersion
		}
		options.ClusterVersion = version
	}
}

// WithSASL enables SASL authentication.
func WithSASL(mechanism, username, password string) Option {
	return func(options *Options) {
		options.sasl = newSASL(mechanism, username, password)
	}
}

// WithLogWriter sets the writer to write the internal Sarama logs to.
func WithLogWriter(writer io.Writer) Option {
	return func(options *Options) {
		options.logWriter = writer
	}
}

// WithTLS enables TLS.
func WithTLS(tls *tls.Config) Option {
	return func(options *Options) {
		options.TLS = tls
	}
}
