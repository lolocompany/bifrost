package metrics_test

import (
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
)

func TestRegisteredMetricNamesUseErrorsTerminology(t *testing.T) {
	enabled := true
	bridges := []bifrostconfig.Bridge{
		{
			Name: "a-to-b",
			From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
			To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
		},
	}
	mr, err := metrics.NewFromConfig(bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{
			Enable:     &enabled,
			ListenAddr: "127.0.0.1:0",
		},
		Bridges: bridges,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	defer mr.StopServer()

	fams, err := mr.Gather()
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
	enabled := true
	bridges := []bifrostconfig.Bridge{
		{
			Name: "a-to-b",
			From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
			To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
		},
	}
	mr, err := metrics.NewFromConfig(bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{
			Enable:     &enabled,
			ListenAddr: "127.0.0.1:0",
		},
		Bridges: bridges,
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	defer mr.StopServer()

	m := mr.BridgeMetrics
	m.IncMessages(bridge.IdentityFrom(bridges[0]))

	fams, err := mr.Gather()
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
