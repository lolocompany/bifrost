package kafka_test

import (
	"context"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/kafka"
)

// Must match default ping timeout when cluster.client.dial_timeout is unset (see pkg/kafka.WithPingTimeout).
const defaultPingTimeout = 30 * time.Second

func TestWithPingTimeout_default(t *testing.T) {
	t.Parallel()
	ctx, cancel, err := kafka.WithPingTimeout(context.Background(), &config.Cluster{})
	if err != nil {
		t.Fatalf("WithPingTimeout: %v", err)
	}
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected deadline")
	}
	if rem := time.Until(deadline); rem <= 0 || rem > defaultPingTimeout {
		t.Fatalf("expected remaining in (0, %v], got %v", defaultPingTimeout, rem)
	}
}

func TestWithPingTimeout_fromDialTimeout(t *testing.T) {
	t.Parallel()
	ctx, cancel, err := kafka.WithPingTimeout(context.Background(), &config.Cluster{
		Client: config.ClientSettings{DialTimeout: "5s"},
	})
	if err != nil {
		t.Fatalf("WithPingTimeout: %v", err)
	}
	defer cancel()
	deadline, ok := ctx.Deadline()
	if !ok {
		t.Fatal("expected deadline")
	}
	if rem := time.Until(deadline); rem <= 0 || rem > 5*time.Second {
		t.Fatalf("expected remaining in (0, 5s], got %v", rem)
	}
}

func TestWithPingTimeout_invalidDialTimeout(t *testing.T) {
	t.Parallel()
	_, _, err := kafka.WithPingTimeout(context.Background(), &config.Cluster{
		Client: config.ClientSettings{DialTimeout: "not-a-duration"},
	})
	if err == nil {
		t.Fatal("expected error")
	}
}
