package metrics_test

import (
	"testing"

	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
)

func TestBridgeIdentityLabelCountMatchesVec(t *testing.T) {
	labelNames := bridge.LabelNames()
	id := bridge.IdentityFrom(bifrostconfig.Bridge{
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
