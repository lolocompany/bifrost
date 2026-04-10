package metrics

// TCPMetricNames lists all TCP metric names emitted from broker hook instrumentation.
var TCPMetricNames = []string{
	"bifrost_tcp_connect_attempts_total",
	"bifrost_tcp_connect_errors_total",
	"bifrost_tcp_connect_duration_seconds",
	"bifrost_tcp_disconnects_total",
	"bifrost_tcp_active_connections",
}
