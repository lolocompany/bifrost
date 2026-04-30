package metrics_test

import (
	"testing"

	bifrostconfig "github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/domain/relay"
)

func TestBridgeIdentityLabelCountMatchesVec(t *testing.T) {
	labelNames := relay.LabelNames()
	id := relayIdentityFromBridge(bifrostconfig.Bridge{
		Name: "b1",
		From: bifrostconfig.BridgeTarget{
			Cluster: "src",
			Topic:   "t.in",
		},
		To: bifrostconfig.BridgeTarget{
			Cluster: "dst",
			Topic:   "t.out",
		},
	})
	if len(labelNames) != len(id.LabelValues()) {
		t.Fatalf("LabelNames has %d names but LabelValues() has %d values", len(labelNames), len(id.LabelValues()))
	}
	wantNames := []string{"bridge", "from_kafka_cluster", "from_topic", "to_kafka_cluster", "to_topic"}
	for i, want := range wantNames {
		if labelNames[i] != want {
			t.Fatalf("LabelNames[%d] = %q, want %q", i, labelNames[i], want)
		}
	}
}
