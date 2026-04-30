package metrics_test

import (
	"strings"
	"testing"
)

const tcpMetricPrefix = "bifrost_tcp_"

var tcpMetricNames = []string{
	"bifrost_tcp_connect_attempts_total",
	"bifrost_tcp_connect_errors_total",
	"bifrost_tcp_connect_duration_seconds",
	"bifrost_tcp_disconnects_total",
	"bifrost_tcp_active_connections",
}

func TestTCPMetricNamesNoRepeatedPrefix(t *testing.T) {
	for _, name := range tcpMetricNames {
		if strings.Contains(name, "tcp_tcp") {
			t.Errorf("metric %q repeats the tcp_ prefix; use a single tcp_ segment", name)
		}
		if !strings.HasPrefix(name, tcpMetricPrefix) {
			t.Errorf("metric %q must start with %s", name, tcpMetricPrefix)
		}
	}
}
