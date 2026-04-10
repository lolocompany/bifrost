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

func TestRegisteredMetricNamesUseRelaySubsystem(t *testing.T) {
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
	m, _, err := metrics.New(reg, cfg, bridges)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	m.AddForwarded(metrics.BridgeIdentityFrom(bridges[0]))

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}

	seenRelayMessages := false
	for _, mf := range fams {
		name := mf.GetName()
		if name == "bifrost_relay_messages_total" {
			seenRelayMessages = true
		}
		if strings.HasPrefix(name, "bifrost_forward_") ||
			strings.HasPrefix(name, "bifrost_errors_") ||
			strings.HasPrefix(name, "bifrost_latency_") {
			t.Fatalf("metric %q must use relay subsystem", name)
		}
	}
	if !seenRelayMessages {
		t.Fatal("expected bifrost_relay_messages_total to be registered")
	}
}
