//go:build linux

package metrics_test

import (
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/pkg/metrics"
)

func TestTCPMetricNamesNoRepeatedPrefix(t *testing.T) {
	for _, name := range metrics.TCPMetricNames {
		if strings.Contains(name, "tcp_tcp") {
			t.Errorf("metric %q repeats the tcp_ prefix; use a single tcp_ segment (e.g. tcp_netstat_*)", name)
		}
		if !strings.HasPrefix(name, "bifrost_tcp_") {
			t.Errorf("metric %q must start with bifrost_tcp_", name)
		}
	}
}
