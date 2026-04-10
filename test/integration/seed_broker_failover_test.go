package integration_test

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/kafka"
)

// TestClusterBrokerFailover_SecondSeedReachable checks that when the first seed broker in
// clusters.*.brokers refuses connections, franz-go still connects via a later seed (same
// behavior bifrost relies on at startup via [kafka.PingBroker]).
func TestClusterBrokerFailover_SecondSeedReachable(t *testing.T) {
	requireIntegration(t)

	ctx := context.Background()
	ctr, err := redpanda.Run(ctx, redpandaImage, redpanda.WithAutoCreateTopics())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("redpanda: %v", err)
	}

	good, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("KafkaSeedBroker: %v", err)
	}

	// Port 1 is not a Kafka listener; dial should fail quickly so we exercise failover to the next seed.
	brokers := []string{"127.0.0.1:1", good}

	env := &bifrostconfig.Cluster{
		Brokers: brokers,
		TLS:     bifrostconfig.TLS{Enabled: false},
		SASL:    bifrostconfig.SASL{Mechanism: "none"},
		Client:  bifrostconfig.ClientSettings{DialTimeout: "15s"},
	}

	producer, err := kafka.NewProducer(env, nil)
	if err != nil {
		t.Fatalf("NewProducer: %v", err)
	}
	defer producer.Close()

	pingCtx, cancel, err := kafka.WithPingTimeout(ctx, env)
	if err != nil {
		t.Fatalf("WithPingTimeout: %v", err)
	}
	defer cancel()

	if err := kafka.PingBroker(pingCtx, producer); err != nil {
		t.Fatalf("PingBroker with [unreachable, good] seeds: %v", err)
	}
}
