package kafka

import (
	"github.com/xitonix/trubka/internal"
)

const (
	DefaultClusterVersion = "2.1.1"
)

// Options holds the configuration settings for kafka consumer.
type Options struct {
	// DisableErrorReporting disables sending consumer errors to the Errors() channel.
	DisableErrorReporting bool
	// ClusterVersion kafka cluster version.
	ClusterVersion string
	// Rewind if true, the consumer will start consuming from the beginning of the stream.
	Rewind bool
	// OffsetStore the type responsible to store consumer offsets
	OffsetStore OffsetStore
	// ResetOffsets if true, the stored offsets will be reset.
	ResetOffsets bool
}

// NewOptions creates a new Options object with default values.
func NewOptions() *Options {
	return &Options{
		DisableErrorReporting: false,
		ClusterVersion:        DefaultClusterVersion,
		Rewind:                false,
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

// WithRewind if set to true the consumer will start consuming from the beginning of the stream.
func WithRewind(rewind bool) Option {
	return func(options *Options) {
		options.Rewind = rewind
	}
}

// WithOffsetReset resets the stored offset if any.
func WithOffsetReset(reset bool) Option {
	return func(options *Options) {
		options.ResetOffsets = reset
	}
}

// WithOffsetStore sets the consumer offset store.
func WithOffsetStore(store OffsetStore) Option {
	return func(options *Options) {
		options.OffsetStore = store
	}
}
