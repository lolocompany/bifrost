package metrics_test

import (
	"strings"
	"testing"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
)

func TestRegisteredMetricNamesUseErrorsTerminology(t *testing.T) {
	reg := prometheus.NewRegistry()
	enabled := true
	cfg := bifrostconfig.Metrics{Enable: &enabled}
	bridges := []bifrostconfig.Bridge{
		{
			Name: "a-to-b",
			From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
			To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
		},
	}
	if _, _, err := metrics.New(reg, cfg, bridges); err != nil {
		t.Fatalf("metrics.New: %v", err)
	}

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	for _, mf := range fams {
		name := mf.GetName()
		if strings.Contains(name, "failures") {
			t.Fatalf("metric %q must use errors terminology instead of failures", name)
		}
	}
}
