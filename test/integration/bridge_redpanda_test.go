// Integration tests require Docker (Testcontainers) and opt-in:
//
//	BIFROST_INTEGRATION=1 go test ./test/integration/...
//
// Redpanda module: https://golang.testcontainers.org/modules/redpanda/

package integration_test

import (
	"testing"

	kafkautil "github.com/lolocompany/bifrost/test/integration/testutil/kafka"
)

func TestBridgeRelay_Redpanda(t *testing.T) {
	requireIntegration(t)
	t.Run("single relay loop", func(t *testing.T) {
		runBridgeRelayTest(t, kafkautil.ProviderRedpanda)
	})
	t.Run("bifrost process scenarios", func(t *testing.T) {
		runRelayScenarios(t, kafkautil.ProviderRedpanda)
	})
}
