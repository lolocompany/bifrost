package metrics_test

import (
	"testing"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/prometheus/client_golang/prometheus"
)

func TestMetricsExtraLabelsApplied(t *testing.T) {
	reg := prometheus.NewRegistry()
	reger := prometheus.WrapRegistererWith(prometheus.Labels{
		"service": "bifrost",
		"env":     "test",
	}, reg)

	metricsOn := true
	cfg := bifrostconfig.Metrics{
		Enable: &metricsOn,
	}
	bridges := []bifrostconfig.Bridge{
		{
			Name: "b1",
			From: bifrostconfig.BridgeTarget{Cluster: "a", Topic: "in"},
			To:   bifrostconfig.BridgeTarget{Cluster: "b", Topic: "out"},
		},
	}
	m, bp, err := metrics.New(reger, cfg, bridges)
	if err != nil {
		t.Fatalf("metrics.New: %v", err)
	}
	if m == nil || bp == nil {
		t.Fatal("expected bridge and broker metrics to be enabled")
	}

	// Touch a metric from each family so at least one sample exists.
	id := metrics.BridgeIdentityFrom(bridges[0])
	m.AddForwarded(id)
	if h := bp.HookFor("a"); h == nil {
		t.Fatal("expected kafka/tls hook")
	}

	fams, err := reg.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if len(fams) == 0 {
		t.Fatal("no metric families gathered")
	}

	seenRelay := false
	seenGo := false
	for _, mf := range fams {
		switch mf.GetName() {
		case "bifrost_relay_messages_total":
			seenRelay = true
			assertHasLabel(t, mf, "service", "bifrost")
			assertHasLabel(t, mf, "env", "test")
		case "go_goroutines":
			seenGo = true
			assertHasLabel(t, mf, "service", "bifrost")
			assertHasLabel(t, mf, "env", "test")
		}
	}
	if !seenRelay {
		t.Fatal("did not find bifrost_relay_messages_total")
	}
	if !seenGo {
		t.Fatal("did not find go_goroutines")
	}
}

func assertHasLabel(t *testing.T, mf *dto.MetricFamily, key, want string) {
	t.Helper()
	for _, m := range mf.GetMetric() {
		for _, lp := range m.GetLabel() {
			if lp.GetName() == key && lp.GetValue() == want {
				return
			}
		}
	}
	t.Fatalf("metric family %q missing label %s=%q", mf.GetName(), key, want)
}
