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
	// ExtraHeaders are appended after bifrost course/source headers (if enabled) and before
	// optional propagated record headers.
	ExtraHeaders []kgo.RecordHeader
	// SourceHeadersEnabled when nil defaults to true (emit bifrost.course.hash and optional source.*).
	SourceHeadersEnabled *bool
	// SourceHeadersVerbose when true appends bifrost.source.* after the course hash (compact is default).
	SourceHeadersVerbose bool
	// PropagateRecordHeaders when nil defaults to true (copy consumed record headers to output).
	PropagateRecordHeaders *bool
}

// EffectiveSourceHeadersEnabled returns whether bifrost course/source headers are emitted (default true).
func (o Options) EffectiveSourceHeadersEnabled() bool {
	if o.SourceHeadersEnabled == nil {
		return true
	}
	return *o.SourceHeadersEnabled
}

// EffectivePropagateRecordHeaders returns whether inbound record headers are copied to outbound records (default true).
func (o Options) EffectivePropagateRecordHeaders() bool {
	if o.PropagateRecordHeaders == nil {
		return true
	}
	return *o.PropagateRecordHeaders
}
