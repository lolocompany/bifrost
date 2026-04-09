// Kafka (KRaft) module: https://golang.testcontainers.org/modules/kafka/

package integration_test

import (
	"context"
	"testing"

	"github.com/testcontainers/testcontainers-go"
	"github.com/testcontainers/testcontainers-go/modules/kafka"
)

const confluentLocalImage = "confluentinc/confluent-local:7.5.0"

func TestBridgeRelay_KafkaKRaft(t *testing.T) {
	requireIntegration(t)

	ctx := context.Background()
	ctr, err := kafka.Run(ctx, confluentLocalImage, kafka.WithClusterID("bifrost-itest-kafka"))
	testcontainers.CleanupContainer(t, ctr)
	if err != nil {
		t.Fatalf("kafka: %v", err)
	}

	brokers, err := ctr.Brokers(ctx)
	if err != nil {
		t.Fatalf("Brokers: %v", err)
	}

	runBridgeRelayTest(t, brokers)
}
