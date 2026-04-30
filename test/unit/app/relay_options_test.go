package app_test

import (
	"testing"
	"time"

	"github.com/lolocompany/bifrost/internal/app"
	"github.com/lolocompany/bifrost/internal/config"
)

func TestRelayOptionsFromBridge_UsesConfiguredRetryDurations(t *testing.T) {
	overridePartition := int32(8)
	overrideKey := "fixed-key"
	bridgeCfg := config.Bridge{
		BatchSize:         8,
		OverridePartition: &overridePartition,
		OverrideKey:       &overrideKey,
	}
	fromCluster := config.Cluster{
		Consumer: config.ConsumerSettings{
			CommitRetry: config.RetrySettings{
				MinBackoff: "2s",
				MaxBackoff: "8s",
				Jitter:     "250ms",
			},
		},
	}
	toCluster := config.Cluster{
		Producer: config.ProducerSettings{
			Retry: config.RetrySettings{
				MinBackoff: "500ms",
				MaxBackoff: "5s",
				Jitter:     "100ms",
			},
		},
	}

	opts, err := app.RelayOptionsFromBridge(15*time.Second, bridgeCfg, fromCluster, toCluster)
	if err != nil {
		t.Fatalf("RelayOptionsFromBridge: %v", err)
	}

	if got, want := opts.PeriodicStatsInterval, 15*time.Second; got != want {
		t.Fatalf("PeriodicStatsInterval = %v, want %v", got, want)
	}
	if got, want := opts.BatchSize, 8; got != want {
		t.Fatalf("BatchSize = %d, want %d", got, want)
	}
	if opts.OverridePartition == nil || *opts.OverridePartition != 8 {
		t.Fatalf("OverridePartition = %v, want 8", opts.OverridePartition)
	}
	if string(opts.OverrideKey) != "fixed-key" {
		t.Fatalf("OverrideKey = %q, want fixed-key", string(opts.OverrideKey))
	}
	if got, want := opts.Retry.Commit.MinBackoff, 2*time.Second; got != want {
		t.Fatalf("Commit.MinBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Commit.MaxBackoff, 8*time.Second; got != want {
		t.Fatalf("Commit.MaxBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Commit.Jitter, 250*time.Millisecond; got != want {
		t.Fatalf("Commit.Jitter = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Produce.MinBackoff, 500*time.Millisecond; got != want {
		t.Fatalf("Produce.MinBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Produce.MaxBackoff, 5*time.Second; got != want {
		t.Fatalf("Produce.MaxBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Produce.Jitter, 100*time.Millisecond; got != want {
		t.Fatalf("Produce.Jitter = %v, want %v", got, want)
	}
}

func TestRelayOptionsFromBridge_UsesRetryDefaultsWhenOmitted(t *testing.T) {
	opts, err := app.RelayOptionsFromBridge(0, config.Bridge{}, config.Cluster{}, config.Cluster{})
	if err != nil {
		t.Fatalf("RelayOptionsFromBridge: %v", err)
	}

	if got, want := opts.BatchSize, config.DefaultBridgeBatchSize; got != want {
		t.Fatalf("BatchSize = %d, want %d", got, want)
	}
	if opts.OverridePartition != nil {
		t.Fatalf("OverridePartition = %v, want nil", opts.OverridePartition)
	}
	if opts.OverrideKey != nil {
		t.Fatalf("OverrideKey = %q, want nil", string(opts.OverrideKey))
	}
	if got, want := opts.Retry.Commit.MinBackoff, config.DefaultCommitRetry.MinBackoff; got != want {
		t.Fatalf("Commit.MinBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Commit.MaxBackoff, config.DefaultCommitRetry.MaxBackoff; got != want {
		t.Fatalf("Commit.MaxBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Commit.Jitter, config.DefaultCommitRetry.Jitter; got != want {
		t.Fatalf("Commit.Jitter = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Produce.MinBackoff, config.DefaultProducerRetry.MinBackoff; got != want {
		t.Fatalf("Produce.MinBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Produce.MaxBackoff, config.DefaultProducerRetry.MaxBackoff; got != want {
		t.Fatalf("Produce.MaxBackoff = %v, want %v", got, want)
	}
	if got, want := opts.Retry.Produce.Jitter, config.DefaultProducerRetry.Jitter; got != want {
		t.Fatalf("Produce.Jitter = %v, want %v", got, want)
	}
}
