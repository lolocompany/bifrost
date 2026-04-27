package bridge_test

import (
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
)

func TestComputeRetryDelay_BackoffAndJitterBounds(t *testing.T) {
	t.Parallel()
	base, jitter, total := bridge.ComputeRetryDelay(
		5,
		100*time.Millisecond,
		800*time.Millisecond,
		250*time.Millisecond,
		func(time.Duration) time.Duration { return 500 * time.Millisecond },
	)
	if base != 800*time.Millisecond {
		t.Fatalf("base=%v want=800ms", base)
	}
	if jitter != 250*time.Millisecond {
		t.Fatalf("jitter=%v want=250ms", jitter)
	}
	if total != 1050*time.Millisecond {
		t.Fatalf("total=%v want=1050ms", total)
	}
}

func TestComputeRetryDelay_NegativeJitterClamped(t *testing.T) {
	t.Parallel()
	base, jitter, total := bridge.ComputeRetryDelay(
		2,
		time.Second,
		4*time.Second,
		100*time.Millisecond,
		func(time.Duration) time.Duration { return -time.Second },
	)
	if base != 2*time.Second {
		t.Fatalf("base=%v want=2s", base)
	}
	if jitter != 0 {
		t.Fatalf("jitter=%v want=0", jitter)
	}
	if total != base {
		t.Fatalf("total=%v want=%v", total, base)
	}
}
