package metrics

import (
	"crypto/tls"
	"net"

	"github.com/prometheus/client_golang/prometheus"
)

// tlsBrokerMetrics holds Prometheus series for TLS to Kafka brokers.
type tlsBrokerMetrics struct {
	handshakes       *prometheus.CounterVec
	handshakeErrs    *prometheus.CounterVec
	peerLeafNotAfter *prometheus.GaugeVec
}

func newTLSBrokerMetrics(labelCluster []string) *tlsBrokerMetrics {
	t := &tlsBrokerMetrics{}
	t.handshakes = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_tls_handshakes_total",
			Help: "Completed TLS handshakes to Kafka brokers, labeled by bifrost cluster and TLS version.",
		},
		append(append([]string(nil), labelCluster...), "tls_version"),
	)
	t.handshakeErrs = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_tls_handshake_errors_total",
			Help: "TLS connection attempts where the connection was not a completed TLS handshake (or handshake incomplete).",
		},
		labelCluster,
	)
	t.peerLeafNotAfter = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Name: "bifrost_tls_peer_leaf_not_after_timestamp_seconds",
			Help: "Expiry time of the broker leaf certificate as Unix timestamp in seconds since epoch (last observed per cluster).",
		},
		labelCluster,
	)
	return t
}

func (t *tlsBrokerMetrics) collectors() []prometheus.Collector {
	if t == nil {
		return nil
	}
	return []prometheus.Collector{
		t.handshakes,
		t.handshakeErrs,
		t.peerLeafNotAfter,
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
	cur := c
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
