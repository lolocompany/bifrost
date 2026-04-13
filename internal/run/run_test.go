package run

import (
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/config"
)

func TestBridgeRunOptions_UsesConfiguredRetryDurations(t *testing.T) {
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

	opts, err := bridgeRunOptions(15*time.Second, fromCluster, toCluster)
	if err != nil {
		t.Fatalf("bridgeRunOptions: %v", err)
	}

	if got, want := opts.PeriodicStatsInterval, 15*time.Second; got != want {
		t.Fatalf("PeriodicStatsInterval = %v, want %v", got, want)
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

func TestBridgeRunOptions_UsesRetryDefaultsWhenOmitted(t *testing.T) {
	opts, err := bridgeRunOptions(0, config.Cluster{}, config.Cluster{})
	if err != nil {
		t.Fatalf("bridgeRunOptions: %v", err)
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
