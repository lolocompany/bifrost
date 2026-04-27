// Kafka (KRaft) module: https://golang.testcontainers.org/modules/kafka/

package integration_test

import (
	"testing"

	kafkautil "github.com/lolocompany/bifrost/test/integration/testutil/kafka"
)

func TestBridgeRelay_KafkaKRaft(t *testing.T) {
	requireIntegration(t)
	t.Run("single relay loop", func(t *testing.T) {
		runBridgeRelayTest(t, kafkautil.ProviderKafka)
	})
	t.Run("bifrost process scenarios", func(t *testing.T) {
		runRelayScenarios(t, kafkautil.ProviderKafka)
	})
}
