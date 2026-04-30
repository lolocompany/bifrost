package metrics

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/lolocompany/bifrost/internal/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
)

// BrokerMetrics holds Prometheus series for Kafka broker connections (wire protocol, TLS, TCP).
// Each subgroup is non-nil only when the corresponding metrics group is enabled in config.
type BrokerMetrics struct {
	Kafka *kafkaBrokerMetrics
	TLS   *tlsBrokerMetrics
	TCP   *tcpBrokerMetrics
}

// newBrokerMetrics registers Kafka, TLS, and TCP metrics for broker connections when the corresponding groups are enabled.
// It returns (nil, nil) when Kafka, TLS, and TCP groups are all disabled.
func newBrokerMetrics(reg prometheus.Registerer, g config.MetricGroups) (BrokerMetrics, error) {
	kafkaOn := g.GroupKafka()
	tlsOn := g.GroupTLS()
	tcpOn := g.GroupTCP()
	if !kafkaOn && !tlsOn && !tcpOn {
		return BrokerMetrics{}, nil
	}

	labelCluster := []string{"kafka_cluster"}
	bm := BrokerMetrics{}
	if kafkaOn {
		bm.Kafka = newKafkaBrokerMetrics(labelCluster)
	}
	if tlsOn {
		bm.TLS = newTLSBrokerMetrics(labelCluster)
	}
	if tcpOn {
		bm.TCP = newTCPBrokerMetrics(labelCluster)
	}

	for _, c := range bm.collectors() {
		if err := reg.Register(c); err != nil {
			return BrokerMetrics{}, fmt.Errorf("register broker metric: %w", err)
		}
	}
	return bm, nil
}

func (bm *BrokerMetrics) collectors() []prometheus.Collector {
	if bm == nil {
		return nil
	}
	var out []prometheus.Collector
	out = append(out, bm.Kafka.collectors()...)
	out = append(out, bm.TLS.collectors()...)
	out = append(out, bm.TCP.collectors()...)
	return out
}

// HookFor returns a franz-go hook instance for one bifrost cluster name (re-use on all clients for that cluster).
func (bm *BrokerMetrics) HookFor(cluster string) kgo.Hook {
	if bm == nil {
		return nil
	}
	return &franzHook{cluster: cluster, bm: bm}
}

// RecordTCPConnectDuration records bifrost_tcp_connect_duration_seconds (TCP dial only, before TLS).
func (bm *BrokerMetrics) RecordTCPConnectDuration(cluster string, seconds float64) {
	if bm == nil || bm.TCP == nil {
		return
	}
	bm.TCP.connectDuration.WithLabelValues(cluster).Observe(seconds)
}

// TCPDialRecorder returns a callback for [kafka.ClientOpts] when the TCP metrics group is enabled;
// otherwise it returns nil so franz-go uses its default dial path.
func (bm *BrokerMetrics) TCPDialRecorder(cluster string) func(float64) {
	if bm == nil || bm.TCP == nil {
		return nil
	}
	return func(seconds float64) {
		bm.RecordTCPConnectDuration(cluster, seconds)
	}
}

// franzHook implements kgo broker hooks for Prometheus.
type franzHook struct {
	cluster string
	bm      *BrokerMetrics
	mu      sync.Mutex
	conns   map[net.Conn]struct{}
}

var (
	_ kgo.HookBrokerConnect    = (*franzHook)(nil)
	_ kgo.HookBrokerDisconnect = (*franzHook)(nil)
	_ kgo.HookBrokerE2E        = (*franzHook)(nil)
	_ kgo.HookBrokerThrottle   = (*franzHook)(nil)
)

func (f *franzHook) OnBrokerConnect(_ kgo.BrokerMetadata, initDur time.Duration, conn net.Conn, err error) {
	if f == nil || f.bm == nil {
		return
	}
	cname := f.cluster
	bm := f.bm

	if k := bm.Kafka; k != nil {
		k.connectAttempts.WithLabelValues(cname).Inc()
		if err != nil {
			k.connectErrors.WithLabelValues(cname).Inc()
		} else {
			k.connectDuration.WithLabelValues(cname).Observe(initDur.Seconds())
		}
	}

	if t := bm.TCP; t != nil {
		t.connectAttempts.WithLabelValues(cname).Inc()
		if err != nil {
			t.connectErrors.WithLabelValues(cname).Inc()
		} else {
			f.mu.Lock()
			if f.conns == nil {
				f.conns = make(map[net.Conn]struct{})
			}
			if _, exists := f.conns[conn]; !exists {
				f.conns[conn] = struct{}{}
				t.activeConns.WithLabelValues(cname).Inc()
			}
			f.mu.Unlock()
		}
	}

	if bm.TLS == nil || conn == nil || err != nil {
		return
	}
	tc := tlsConn(conn)
	if tc == nil {
		return
	}
	st := tc.ConnectionState()
	if !st.HandshakeComplete {
		bm.TLS.handshakeErrs.WithLabelValues(cname).Inc()
		return
	}
	bm.TLS.handshakes.WithLabelValues(cname, tlsVersionLabel(st.Version)).Inc()
	if len(st.PeerCertificates) > 0 {
		bm.TLS.peerLeafNotAfter.WithLabelValues(cname).Set(float64(st.PeerCertificates[0].NotAfter.Unix()))
	}
}

func (f *franzHook) OnBrokerE2E(_ kgo.BrokerMetadata, _ int16, e2e kgo.BrokerE2E) {
	if f == nil || f.bm == nil || f.bm.Kafka == nil {
		return
	}
	cname := f.cluster
	k := f.bm.Kafka

	k.e2eRequests.WithLabelValues(cname).Inc()
	k.e2eWriteBytes.WithLabelValues(cname).Add(float64(e2e.BytesWritten))
	k.e2eReadBytes.WithLabelValues(cname).Add(float64(e2e.BytesRead))
	k.e2eLatency.WithLabelValues(cname).Observe(e2e.DurationE2E().Seconds())
	if e2e.Err() != nil {
		k.e2eErrors.WithLabelValues(cname).Inc()
	}
}

func (f *franzHook) OnBrokerThrottle(_ kgo.BrokerMetadata, throttleInterval time.Duration, _ bool) {
	if f == nil || f.bm == nil || f.bm.Kafka == nil {
		return
	}
	cname := f.cluster
	k := f.bm.Kafka
	k.throttleSeconds.WithLabelValues(cname).Add(throttleInterval.Seconds())
	k.throttleEvents.WithLabelValues(cname).Inc()
}

func (f *franzHook) OnBrokerDisconnect(_ kgo.BrokerMetadata, conn net.Conn) {
	if f == nil || f.bm == nil || f.bm.TCP == nil {
		return
	}
	cname := f.cluster
	t := f.bm.TCP
	t.disconnects.WithLabelValues(cname).Inc()
	f.mu.Lock()
	if _, exists := f.conns[conn]; exists {
		delete(f.conns, conn)
		t.activeConns.WithLabelValues(cname).Dec()
	}
	f.mu.Unlock()
}
