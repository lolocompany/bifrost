// Integration tests require Docker (Testcontainers) and opt-in:
//
//	BIFROST_INTEGRATION=1 go test ./test/integration/...
//
// Redpanda module: https://golang.testcontainers.org/modules/redpanda/

package integration_test

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/redpanda"
)

const redpandaImage = "docker.redpanda.com/redpandadata/redpanda:v24.3.11"

func TestBridgeRelay_Redpanda(t *testing.T) {
	requireIntegration(t)

	ctx := context.Background()
	ctr, err := redpanda.Run(ctx, redpandaImage, redpanda.WithAutoCreateTopics())
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("redpanda: %v", err)
	}

	seed, err := ctr.KafkaSeedBroker(ctx)
	if err != nil {
		t.Fatalf("KafkaSeedBroker: %v", err)
	}

	runBridgeRelayTest(t, []string{seed})
}
