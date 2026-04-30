package metrics_test

import (
	"strings"
	"testing"

	bifrostconfig "github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/observability/metrics"
)

func TestMetricsExtraLabelsBuiltInVariableLabelCollisionFailsRegistration(t *testing.T) {
	metricsOn := true
	bridges := []bifrostconfig.Bridge{
		{
			Name: "b1",
			From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
			To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
		},
	}
	_, err := metrics.NewFromConfig(bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{
			Enable:      &metricsOn,
			ListenAddr:  "127.0.0.1:0",
			ExtraLabels: map[string]string{"bridge": "shadow"},
		},
		Bridges: bridges,
	})
	if err == nil {
		t.Fatal("expected error for extra_labels collision with built-in variable label")
	}
	if !strings.Contains(err.Error(), "duplicate label names") {
		t.Fatalf("error = %v, want duplicate label names conflict", err)
	}
}
