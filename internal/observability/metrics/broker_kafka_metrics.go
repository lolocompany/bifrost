package metrics

import "github.com/prometheus/client_golang/prometheus"

// kafkaBrokerMetrics holds Prometheus series for Kafka wire protocol (connect, E2E, throttle).
type kafkaBrokerMetrics struct {
	connectAttempts *prometheus.CounterVec
	connectErrors   *prometheus.CounterVec
	connectDuration *prometheus.HistogramVec

	e2eRequests   *prometheus.CounterVec
	e2eErrors     *prometheus.CounterVec
	e2eWriteBytes *prometheus.CounterVec
	e2eReadBytes  *prometheus.CounterVec
	e2eLatency    *prometheus.HistogramVec

	throttleSeconds *prometheus.CounterVec
	throttleEvents  *prometheus.CounterVec
}

func newKafkaBrokerMetrics(labelCluster []string) *kafkaBrokerMetrics {
	k := &kafkaBrokerMetrics{}
	k.connectAttempts = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_connect_attempts_total",
			Help: "Broker dial attempts (before success or failure), labeled by bifrost cluster name.",
		},
		labelCluster,
	)
	k.connectErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_connect_errors_total",
			Help: "Broker dial or init errors (including TLS/SASL handshake).",
		},
		labelCluster,
	)
	k.connectDuration = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bifrost_kafka_connect_duration_seconds",
			Help:    "Duration in seconds to dial and initialize a broker connection (incl. API versions and SASL).",
			Buckets: prometheus.DefBuckets,
		},
		labelCluster,
	)
	k.e2eRequests = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_requests_total",
			Help: "Completed broker request/response pairs observed via franz-go E2E hook.",
		},
		labelCluster,
	)
	k.e2eErrors = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_request_errors_total",
			Help: "Broker E2E request/response errors (write or read).",
		},
		labelCluster,
	)
	k.e2eWriteBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_write_bytes_total",
			Help: "Bytes written to brokers for full requests (from E2E hook).",
		},
		labelCluster,
	)
	k.e2eReadBytes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_read_bytes_total",
			Help: "Bytes read from brokers for full responses (from E2E hook).",
		},
		labelCluster,
	)
	k.e2eLatency = prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bifrost_kafka_request_duration_seconds",
			Help:    "End-to-end duration in seconds for a broker request write through response read (franz-go E2E hook).",
			Buckets: prometheus.DefBuckets,
		},
		labelCluster,
	)
	k.throttleSeconds = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_throttle_seconds_total",
			Help: "Total accumulated broker throttle time in seconds applied to this client.",
		},
		labelCluster,
	)
	k.throttleEvents = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_kafka_throttle_events_total",
			Help: "Number of throttled broker responses.",
		},
		labelCluster,
	)
	return k
}

func (k *kafkaBrokerMetrics) collectors() []prometheus.Collector {
	if k == nil {
		return nil
	}
	return []prometheus.Collector{
		k.connectAttempts,
		k.connectErrors,
		k.connectDuration,
		k.e2eRequests,
		k.e2eErrors,
		k.e2eWriteBytes,
		k.e2eReadBytes,
		k.e2eLatency,
		k.throttleSeconds,
		k.throttleEvents,
	}
}
