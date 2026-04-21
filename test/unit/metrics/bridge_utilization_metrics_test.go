package metrics_test

import (
	"math"
	"testing"

	dto "github.com/prometheus/client_model/go"

	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
)

func TestBridgeUtilizationMetricsRegisteredWithStateLabel(t *testing.T) {
	enabled := true
	cfg := bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{Enable: &enabled, ListenAddr: "127.0.0.1:0"},
		Bridges: []bifrostconfig.Bridge{{
			Name: "a-to-b",
			From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
			To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
		}},
	}
	mr, err := metrics.NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	defer mr.StopServer()

	fams, err := mr.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if _, ok := findMetricFamily(fams, "bifrost_relay_consumer_seconds_total"); !ok {
		t.Fatal("missing bifrost_relay_consumer_seconds_total")
	}
	if _, ok := findMetricFamily(fams, "bifrost_relay_producer_seconds_total"); !ok {
		t.Fatal("missing bifrost_relay_producer_seconds_total")
	}
}

func TestBridgeUtilizationMetricsAddExpectedCounterValues(t *testing.T) {
	enabled := true
	br := bifrostconfig.Bridge{
		Name: "a-to-b",
		From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
		To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
	}
	cfg := bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{Enable: &enabled, ListenAddr: "127.0.0.1:0"},
		Bridges: []bifrostconfig.Bridge{br},
	}
	mr, err := metrics.NewFromConfig(cfg)
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	defer mr.StopServer()

	id := bridge.IdentityFrom(br)
	mr.BridgeMetrics.AddConsumerSeconds(id, "busy", 1.25)
	mr.BridgeMetrics.AddConsumerSeconds(id, "idle", 2.5)
	mr.BridgeMetrics.AddProducerSeconds(id, "busy", 3.75)
	mr.BridgeMetrics.AddProducerSeconds(id, "idle", 4.5)

	fams, err := mr.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	base := id.LabelValues()
	assertCounterValue(t, fams, "bifrost_relay_consumer_seconds_total", append(base, "busy"), 1.25)
	assertCounterValue(t, fams, "bifrost_relay_consumer_seconds_total", append(base, "idle"), 2.5)
	assertCounterValue(t, fams, "bifrost_relay_producer_seconds_total", append(base, "busy"), 3.75)
	assertCounterValue(t, fams, "bifrost_relay_producer_seconds_total", append(base, "idle"), 4.5)
}

func assertCounterValue(t *testing.T, fams []*dto.MetricFamily, name string, labels []string, want float64) {
	t.Helper()
	mf, ok := findMetricFamily(fams, name)
	if !ok {
		t.Fatalf("missing metric family %q", name)
	}
	expected := map[string]string{
		"bridge":       labels[0],
		"from_cluster": labels[1],
		"from_topic":   labels[2],
		"to_cluster":   labels[3],
		"to_topic":     labels[4],
		"state":        labels[5],
	}
	for _, m := range mf.GetMetric() {
		if metricMatchesLabels(m, expected) {
			got := m.GetCounter().GetValue()
			if math.Abs(got-want) > 1e-9 {
				t.Fatalf("%s%v = %f, want %f", name, labels, got, want)
			}
			return
		}
	}
	t.Fatalf("missing sample for %s%v", name, labels)
}

func findMetricFamily(fams []*dto.MetricFamily, name string) (*dto.MetricFamily, bool) {
	for _, mf := range fams {
		if mf.GetName() == name {
			return mf, true
		}
	}
	return nil, false
}

func metricMatchesLabels(m *dto.Metric, expected map[string]string) bool {
	if len(m.GetLabel()) != len(expected) {
		return false
	}
	for _, lp := range m.GetLabel() {
		want, ok := expected[lp.GetName()]
		if !ok || lp.GetValue() != want {
			return false
		}
	}
	return true
}
