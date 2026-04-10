package metrics_test

import (
	"regexp"
	"strings"
	"testing"
)

// Prometheus metric name syntax (see https://prometheus.io/docs/concepts/data_model/#metric-names-and-labels).
var promNameRE = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// bifrostCounters are Counter/CounterVec metric base names (must end with _total per conventions).
var bifrostCounters = []string{
	"bifrost_relay_messages_total",
	"bifrost_relay_errors_total",
	"bifrost_kafka_connect_attempts_total",
	"bifrost_kafka_connect_errors_total",
	"bifrost_kafka_requests_total",
	"bifrost_kafka_request_errors_total",
	"bifrost_kafka_write_bytes_total",
	"bifrost_kafka_read_bytes_total",
	"bifrost_kafka_throttle_seconds_total",
	"bifrost_kafka_throttle_events_total",
	"bifrost_tls_handshakes_total",
	"bifrost_tls_handshake_errors_total",
	"bifrost_tcp_connect_attempts_total",
	"bifrost_tcp_connect_errors_total",
	"bifrost_tcp_disconnects_total",
}

// bifrostHistograms are Histogram metric base names (unit suffix _seconds or _bytes).
var bifrostHistograms = []string{
	"bifrost_relay_produce_duration_seconds",
	"bifrost_kafka_connect_duration_seconds",
	"bifrost_kafka_request_duration_seconds",
}

// bifrostGauges are Gauge metric base names (no _total suffix; include unit where applicable).
var bifrostGauges = []string{
	"bifrost_tls_peer_leaf_not_after_timestamp_seconds",
}

func TestPrometheusMetricNameSyntax(t *testing.T) {
	for _, name := range append(append(append([]string{}, bifrostCounters...), bifrostHistograms...), bifrostGauges...) {
		if !promNameRE.MatchString(name) {
			t.Errorf("metric name %q does not match Prometheus name syntax", name)
		}
	}
}

func TestCounterNamesEndWithTotal(t *testing.T) {
	for _, name := range bifrostCounters {
		if !strings.HasSuffix(name, "_total") {
			t.Errorf("counter %q should end with _total", name)
		}
	}
}

func TestHistogramNamesHaveUnitSuffix(t *testing.T) {
	for _, name := range bifrostHistograms {
		if !strings.HasSuffix(name, "_seconds") && !strings.HasSuffix(name, "_bytes") {
			t.Errorf("histogram %q should end with _seconds or _bytes (base units)", name)
		}
	}
}

func TestGaugeTimestampName(t *testing.T) {
	for _, name := range bifrostGauges {
		if strings.Contains(name, "timestamp") && !strings.HasSuffix(name, "_timestamp_seconds") {
			t.Errorf("gauge %q should end with _timestamp_seconds for Unix time in seconds (OpenMetrics)", name)
		}
	}
}

func TestMetricNamesDoNotUseFailuresTerm(t *testing.T) {
	names := append(append(append([]string{}, bifrostCounters...), bifrostHistograms...), bifrostGauges...)
	for _, name := range names {
		if strings.Contains(name, "failures") {
			t.Errorf("metric %q must use errors terminology instead of failures", name)
		}
	}
}

func TestMetricNamesDoNotUseForwardErrorsLatencySubsystems(t *testing.T) {
	names := append(append(append([]string{}, bifrostCounters...), bifrostHistograms...), bifrostGauges...)
	for _, name := range names {
		if strings.HasPrefix(name, "bifrost_forward_") ||
			strings.HasPrefix(name, "bifrost_errors_") ||
			strings.HasPrefix(name, "bifrost_latency_") {
			t.Errorf("metric %q must use relay subsystem for core metrics", name)
		}
	}
}
