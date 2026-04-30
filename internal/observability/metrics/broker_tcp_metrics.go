package metrics

import "github.com/prometheus/client_golang/prometheus"

// tcpBrokerMetrics holds Prometheus series for TCP connections to Kafka brokers.
type tcpBrokerMetrics struct {
	connectAttempts *prometheus.CounterVec
	connectErrors   *prometheus.CounterVec
	connectDuration *prometheus.HistogramVec
	disconnects     *prometheus.CounterVec
	activeConns     *prometheus.GaugeVec
}

func newTCPBrokerMetrics(labelCluster []string) *tcpBrokerMetrics {
	t := &tcpBrokerMetrics{}
	t.connectAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_tcp_connect_attempts_total",
			Help: "TCP dial attempts to Kafka brokers by bifrost cluster.",
		},
		labelCluster,
	)
	t.connectErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_tcp_connect_errors_total",
			Help: "TCP dial errors to Kafka brokers by bifrost cluster.",
		},
		labelCluster,
	)
	t.connectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name: "bifrost_tcp_connect_duration_seconds",
			Help: "Wall-clock seconds for the TCP dial to each broker (socket established). " +
				"TLS handshake and SASL are not included. Observed on successful TCP dials before TLS.",
			Buckets: prometheus.DefBuckets,
		},
		labelCluster,
	)
	t.disconnects = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_tcp_disconnects_total",
			Help: "TCP broker disconnect events observed by clients.",
		},
		labelCluster,
	)
	t.activeConns = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bifrost_tcp_active_connections",
			Help: "Current active TCP broker connections by cluster.",
		},
		labelCluster,
	)
	return t
}

func (t *tcpBrokerMetrics) collectors() []prometheus.Collector {
	if t == nil {
		return nil
	}
	return []prometheus.Collector{
		t.connectAttempts,
		t.connectErrors,
		t.connectDuration,
		t.disconnects,
		t.activeConns,
	}
}
