package relay

import (
	"context"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
)

// RetryConfig configures unbounded retry with exponential backoff and additive jitter.
type RetryConfig struct {
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Jitter     time.Duration
	// MaxAttempts bounds retry loops. Values <= 0 keep retrying until context cancellation.
	MaxAttempts int
}

// RetryPolicy defines retry behavior for transient relay stages.
type RetryPolicy struct {
	Produce RetryConfig
	Commit  RetryConfig
}

// Options configures the relay loop.
type Options struct {
	PeriodicStatsInterval time.Duration
	Retry                 RetryPolicy
	BatchSize             int
	MaxInFlightBatches    int
	CommitInterval        time.Duration
	CommitMaxRecords      int
	OverridePartition     *int32
	OverrideKey           []byte
	Sleep                 func(context.Context, time.Duration) error
	Jitter                func(time.Duration) time.Duration
	// ExtraHeaders are appended to each produced record after bifrost.source.* headers and before
	// headers copied from the source record.
	ExtraHeaders []kgo.RecordHeader
}
