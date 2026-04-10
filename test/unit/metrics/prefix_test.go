package metrics_test

import (
	"strings"
	"testing"
)

// Bifrost-owned metric names (excluding std go_* / process_* collectors) must not repeat
// the group prefix (e.g. tcp_tcp, kafka_kafka).
func TestBifrostMetricNamesNoRepeatedGroupPrefix(t *testing.T) {
	names := []string{
		"bifrost_relay_messages_total",
		"bifrost_relay_errors_total",
		"bifrost_relay_produce_duration_seconds",
		"bifrost_kafka_connect_attempts_total",
		"bifrost_kafka_connect_errors_total",
		"bifrost_kafka_connect_duration_seconds",
		"bifrost_kafka_requests_total",
		"bifrost_kafka_request_errors_total",
		"bifrost_kafka_write_bytes_total",
		"bifrost_kafka_read_bytes_total",
		"bifrost_kafka_request_duration_seconds",
		"bifrost_kafka_throttle_seconds_total",
		"bifrost_kafka_throttle_events_total",
		"bifrost_tls_handshakes_total",
		"bifrost_tls_handshake_errors_total",
		"bifrost_tls_peer_leaf_not_after_timestamp_seconds",
		"bifrost_tcp_connect_attempts_total",
		"bifrost_tcp_connect_errors_total",
		"bifrost_tcp_disconnects_total",
		"bifrost_tcp_active_connections",
	}
	stutters := []string{
		"forward_forward",
		"errors_errors",
		"latency_latency",
		"kafka_kafka",
		"tls_tls",
		"tcp_tcp",
	}
	for _, name := range names {
		for _, bad := range stutters {
			if strings.Contains(name, bad) {
				t.Errorf("metric %q must not contain stutter %q", name, bad)
			}
		}
	}
}
