package metrics_test

import (
	"net"
	"reflect"
	"testing"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
	dto "github.com/prometheus/client_model/go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTLSMetrics_plaintextBrokerConnectDoesNotIncrementHandshakeErrors(t *testing.T) {
	enabled := true
	disabled := false

	mr, err := metrics.NewFromConfig(bifrostconfig.Config{
		Metrics: bifrostconfig.Metrics{
			Enable:     &enabled,
			ListenAddr: "127.0.0.1:0",
			Groups: bifrostconfig.MetricGroups{
				Golang:  &disabled,
				Process: &disabled,
				Kafka:   &disabled,
				TLS:     &enabled,
				TCP:     &disabled,
			},
		},
	})
	if err != nil {
		t.Fatalf("NewFromConfig: %v", err)
	}
	defer mr.StopServer()

	brokerMetrics := mr.BrokerMetrics
	if reflect.ValueOf(brokerMetrics).IsZero() {
		t.Fatal("expected broker metrics")
	}

	serverConn, clientConn := net.Pipe()
	defer func() {
		if err := serverConn.Close(); err != nil {
			t.Fatalf("serverConn.Close: %v", err)
		}
	}()
	defer func() {
		if err := clientConn.Close(); err != nil {
			t.Fatalf("clientConn.Close: %v", err)
		}
	}()

	hook := brokerMetrics.HookFor("plain-cluster")
	connectHook, ok := hook.(kgo.HookBrokerConnect)
	if !ok {
		t.Fatalf("hook does not implement HookBrokerConnect: %T", hook)
	}

	connectHook.OnBrokerConnect(kgo.BrokerMetadata{}, 0, serverConn, nil)

	fams, err := mr.Gather()
	if err != nil {
		t.Fatalf("Gather: %v", err)
	}
	if got := counterValueOrZero(fams, "bifrost_tls_handshake_errors_total", "cluster", "plain-cluster"); got != 0 {
		t.Fatalf("handshake error count = %v, want 0", got)
	}
}

func counterValueOrZero(fams []*dto.MetricFamily, familyName, labelName, labelValue string) float64 {
	for _, mf := range fams {
		if mf.GetName() != familyName {
			continue
		}
		for _, metric := range mf.GetMetric() {
			for _, label := range metric.GetLabel() {
				if label.GetName() == labelName && label.GetValue() == labelValue {
					return metric.GetCounter().GetValue()
				}
			}
		}
	}
	return 0
}
