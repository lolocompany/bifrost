package metrics

import (
	"crypto/tls"
	"fmt"
	"net"
	"time"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BrokerProm holds Prometheus series derived from franz-go broker hooks (Kafka wire + TLS).
type BrokerProm struct {
	kafkaEnabled bool
	tlsEnabled   bool

	connectAttempts *prometheus.CounterVec
	connectFailures *prometheus.CounterVec
	connectDuration *prometheus.HistogramVec

	e2eRequests   *prometheus.CounterVec
	e2eErrors     *prometheus.CounterVec
	e2eWriteBytes *prometheus.CounterVec
	e2eReadBytes  *prometheus.CounterVec
	e2eLatency    *prometheus.HistogramVec

	throttleSeconds *prometheus.CounterVec
	throttleEvents  *prometheus.CounterVec

	tlsHandshakes     *prometheus.CounterVec
	tlsHandshakeErrs  *prometheus.CounterVec
	peerLeafNotAfter  *prometheus.GaugeVec
}

// NewBrokerProm registers Kafka/TLS hook metrics when the corresponding groups are enabled.
// It returns (nil, nil) when both Kafka and TLS hook metrics are disabled.
func NewBrokerProm(reg prometheus.Registerer, g bifrostconfig.MetricGroups) (*BrokerProm, error) {
	kafkaOn := g.GroupKafka()
	tlsOn := g.GroupTLS()
	if !kafkaOn && !tlsOn {
		return nil, nil
	}

	bp := &BrokerProm{kafkaEnabled: kafkaOn, tlsEnabled: tlsOn}
	labelCluster := []string{"cluster"}

	if kafkaOn {
		bp.connectAttempts = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_connect_attempts_total",
				Help: "Broker dial attempts (before success or failure), labeled by bifrost cluster name.",
			},
			labelCluster,
		)
		bp.connectFailures = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_connect_failures_total",
				Help: "Broker dial or init failures (including TLS/SASL handshake).",
			},
			labelCluster,
		)
		bp.connectDuration = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "bifrost_kafka_broker_connect_duration_seconds",
				Help:    "Duration in seconds to dial and initialize a broker connection (incl. API versions and SASL).",
				Buckets: prometheus.DefBuckets,
			},
			labelCluster,
		)
		bp.e2eRequests = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_e2e_requests_total",
				Help: "Completed broker request/response pairs observed via franz-go E2E hook.",
			},
			labelCluster,
		)
		bp.e2eErrors = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_e2e_errors_total",
				Help: "Broker E2E request/response errors (write or read).",
			},
			labelCluster,
		)
		bp.e2eWriteBytes = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_e2e_write_bytes_total",
				Help: "Bytes written to brokers for full requests (from E2E hook).",
			},
			labelCluster,
		)
		bp.e2eReadBytes = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_e2e_read_bytes_total",
				Help: "Bytes read from brokers for full responses (from E2E hook).",
			},
			labelCluster,
		)
		bp.e2eLatency = prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "bifrost_kafka_broker_e2e_request_duration_seconds",
				Help:    "End-to-end duration in seconds for a broker request write through response read (franz-go E2E hook).",
				Buckets: prometheus.DefBuckets,
			},
			labelCluster,
		)
		bp.throttleSeconds = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_throttle_seconds_total",
				Help: "Total accumulated broker throttle time in seconds applied to this client.",
			},
			labelCluster,
		)
		bp.throttleEvents = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_kafka_broker_throttle_events_total",
				Help: "Number of throttled broker responses.",
			},
			labelCluster,
		)
	}

	if tlsOn {
		bp.tlsHandshakes = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_tls_broker_handshakes_total",
				Help: "Completed TLS handshakes to Kafka brokers, labeled by bifrost cluster and TLS version.",
			},
			[]string{"cluster", "tls_version"},
		)
		bp.tlsHandshakeErrs = prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_tls_broker_handshake_errors_total",
				Help: "TLS connection attempts where the connection was not a completed TLS handshake (or handshake incomplete).",
			},
			labelCluster,
		)
		bp.peerLeafNotAfter = prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Name: "bifrost_tls_broker_peer_leaf_not_after_timestamp_seconds",
				Help: "Expiry time of the broker leaf certificate as Unix timestamp in seconds since epoch (last observed per cluster).",
			},
			labelCluster,
		)
	}

	for _, c := range bp.collectors() {
		if err := reg.Register(c); err != nil {
			return nil, fmt.Errorf("register broker metric: %w", err)
		}
	}
	return bp, nil
}

func (bp *BrokerProm) collectors() []prometheus.Collector {
	if bp == nil {
		return nil
	}
	raw := []prometheus.Collector{
		bp.connectAttempts,
		bp.connectFailures,
		bp.connectDuration,
		bp.e2eRequests,
		bp.e2eErrors,
		bp.e2eWriteBytes,
		bp.e2eReadBytes,
		bp.e2eLatency,
		bp.throttleSeconds,
		bp.throttleEvents,
		bp.tlsHandshakes,
		bp.tlsHandshakeErrs,
		bp.peerLeafNotAfter,
	}
	var out []prometheus.Collector
	for _, c := range raw {
		if c != nil {
			out = append(out, c)
		}
	}
	return out
}

// HookFor returns a franz-go hook instance for one bifrost cluster name (re-use on all clients for that cluster).
func (bp *BrokerProm) HookFor(cluster string) kgo.Hook {
	if bp == nil {
		return nil
	}
	return &franzHook{cluster: cluster, bp: bp}
}

// franzHook implements kgo broker hooks for Prometheus.
type franzHook struct {
	cluster string
	bp      *BrokerProm
}

var (
	_ kgo.HookBrokerConnect   = (*franzHook)(nil)
	_ kgo.HookBrokerE2E       = (*franzHook)(nil)
	_ kgo.HookBrokerThrottle  = (*franzHook)(nil)
)

func (f *franzHook) OnBrokerConnect(_ kgo.BrokerMetadata, initDur time.Duration, conn net.Conn, err error) {
	if f == nil || f.bp == nil {
		return
	}
	cname := f.cluster
	bp := f.bp

	if bp.kafkaEnabled {
		bp.connectAttempts.WithLabelValues(cname).Inc()
		if err != nil {
			bp.connectFailures.WithLabelValues(cname).Inc()
		} else {
			bp.connectDuration.WithLabelValues(cname).Observe(initDur.Seconds())
		}
	}

	if !bp.tlsEnabled || conn == nil || err != nil {
		return
	}
	tc := tlsConn(conn)
	if tc == nil {
		bp.tlsHandshakeErrs.WithLabelValues(cname).Inc()
		return
	}
	st := tc.ConnectionState()
	if !st.HandshakeComplete {
		bp.tlsHandshakeErrs.WithLabelValues(cname).Inc()
		return
	}
	bp.tlsHandshakes.WithLabelValues(cname, tlsVersionLabel(st.Version)).Inc()
	if len(st.PeerCertificates) > 0 {
		bp.peerLeafNotAfter.WithLabelValues(cname).Set(float64(st.PeerCertificates[0].NotAfter.Unix()))
	}
}

func tlsConn(c net.Conn) *tls.Conn {
	if c == nil {
		return nil
	}
	if tc, ok := c.(*tls.Conn); ok {
		return tc
	}
	type unwrap interface {
		Unwrap() net.Conn
	}
	var cur net.Conn = c
	for i := 0; i < 8 && cur != nil; i++ {
		if tc, ok := cur.(*tls.Conn); ok {
			return tc
		}
		u, ok := cur.(unwrap)
		if !ok {
			return nil
		}
		cur = u.Unwrap()
	}
	return nil
}

func tlsVersionLabel(v uint16) string {
	switch v {
	case tls.VersionTLS12:
		return "1.2"
	case tls.VersionTLS13:
		return "1.3"
	default:
		return "unknown"
	}
}

func (f *franzHook) OnBrokerE2E(_ kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	if f == nil || f.bp == nil || !f.bp.kafkaEnabled {
		return
	}
	cname := f.cluster
	bp := f.bp

	bp.e2eRequests.WithLabelValues(cname).Inc()
	bp.e2eWriteBytes.WithLabelValues(cname).Add(float64(e2e.BytesWritten))
	bp.e2eReadBytes.WithLabelValues(cname).Add(float64(e2e.BytesRead))
	bp.e2eLatency.WithLabelValues(cname).Observe(e2e.DurationE2E().Seconds())
	if e2e.Err() != nil {
		bp.e2eErrors.WithLabelValues(cname).Inc()
	}
}

func (f *franzHook) OnBrokerThrottle(_ kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	if f == nil || f.bp == nil || !f.bp.kafkaEnabled {
		return
	}
	cname := f.cluster
	f.bp.throttleSeconds.WithLabelValues(cname).Add(throttleInterval.Seconds())
	f.bp.throttleEvents.WithLabelValues(cname).Inc()
}
