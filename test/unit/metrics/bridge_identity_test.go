package metrics_test

import (
	"testing"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
)

func TestBridgeIdentityLabelCountMatchesVec(t *testing.T) {
	id := metrics.BridgeIdentityFrom(bifrostconfig.Bridge{
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
	if len(metrics.BridgeLabelNames) != len(id.LabelValues()) {
		t.Fatalf("BridgeLabelNames has %d names but LabelValues() has %d values", len(metrics.BridgeLabelNames), len(id.LabelValues()))
	}
}
