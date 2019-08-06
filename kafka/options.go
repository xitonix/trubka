package kafka

import (
	"crypto/tls"

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
	// OffsetStore the type responsible to store consumer offsets
	OffsetStore OffsetStore
	// TLS configuration to connect to Kafka cluster.
	TLS  *tls.Config
	sasl *sasl
}

// NewOptions creates a new Options object with default values.
func NewOptions() *Options {
	return &Options{
		DisableErrorReporting: false,
		ClusterVersion:        DefaultClusterVersion,
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

// WithOffsetStore sets the consumer offset store.
func WithOffsetStore(store OffsetStore) Option {
	return func(options *Options) {
		options.OffsetStore = store
	}
}

// WithSASL enables SASL authentication.
func WithSASL(mechanism, username, password string) Option {
	return func(options *Options) {
		options.sasl = newSASL(mechanism, username, password)
	}
}

// WithTLS enables TLS.
func WithTLS(tls *tls.Config) Option {
	return func(options *Options) {
		options.TLS = tls
	}
}
