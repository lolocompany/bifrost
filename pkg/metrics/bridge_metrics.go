package metrics

import (
	"fmt"

	"github.com/lolocompany/bifrost/pkg/bridge"
	"github.com/lolocompany/bifrost/pkg/config"

	"github.com/prometheus/client_golang/prometheus"
)

// ASSERT: BridgeMetrics implements bridge.MetricsReporter.
var _ bridge.MetricsReporter = BridgeMetrics{}

// BridgeMetrics holds per-bridge Prometheus series: messages, errors by stage, produce duration.
type BridgeMetrics struct {
	messages        *prometheus.CounterVec
	errors          *prometheus.CounterVec
	produceDuration *prometheus.HistogramVec
	consumerSeconds *prometheus.CounterVec
	producerSeconds *prometheus.CounterVec
}

func newBridgeMetrics(reg prometheus.Registerer, bridges []config.Bridge) (BridgeMetrics, error) {
	m := BridgeMetrics{}

	c := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_relay_messages_total",
			Help: "Total number of messages successfully produced on the to-side cluster.",
		},
		bridge.LabelNames,
	)
	if err := reg.Register(c); err != nil {
		return BridgeMetrics{}, fmt.Errorf("register relay messages counter: %w", err)
	}
	m.messages = c

	cv := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_relay_errors_total",
			Help: "Total number of relay errors by stage (poll, produce, commit, route).",
		},
		append(append([]string(nil), bridge.LabelNames...), "stage"),
	)
	if err := reg.Register(cv); err != nil {
		return BridgeMetrics{}, fmt.Errorf("register relay errors counter: %w", err)
	}
	m.errors = cv

	h := prometheus.NewHistogramVec(
		prometheus.HistogramOpts{
			Name:    "bifrost_relay_produce_duration_seconds",
			Help:    "Wall-clock duration in seconds to produce a relayed record on the to-side cluster.",
			Buckets: prometheus.DefBuckets,
		},
		bridge.LabelNames,
	)
	if err := reg.Register(h); err != nil {
		return BridgeMetrics{}, fmt.Errorf("register relay produce duration histogram: %w", err)
	}
	m.produceDuration = h

	consumerSeconds := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_relay_consumer_seconds_total",
			Help: "Wall-clock seconds attributed to consumer state by bridge (busy or idle).",
		},
		append(append([]string(nil), bridge.LabelNames...), "state"),
	)
	if err := reg.Register(consumerSeconds); err != nil {
		return BridgeMetrics{}, fmt.Errorf("register relay consumer seconds counter: %w", err)
	}
	m.consumerSeconds = consumerSeconds

	producerSeconds := prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Name: "bifrost_relay_producer_seconds_total",
			Help: "Wall-clock seconds attributed to producer state by bridge (busy or idle).",
		},
		append(append([]string(nil), bridge.LabelNames...), "state"),
	)
	if err := reg.Register(producerSeconds); err != nil {
		return BridgeMetrics{}, fmt.Errorf("register relay producer seconds counter: %w", err)
	}
	m.producerSeconds = producerSeconds

	initBridgeSeries(m, bridges)
	return m, nil
}

func initBridgeSeries(m BridgeMetrics, bridges []config.Bridge) {
	for _, br := range bridges {
		id := bridge.IdentityFrom(br)
		v := id.LabelValues()
		if m.messages != nil {
			m.messages.WithLabelValues(v...).Add(0)
		}
		if m.errors != nil {
			for _, stage := range []string{bridge.StagePoll, bridge.StageProduce, bridge.StageCommit, bridge.StageRoute} {
				m.errors.WithLabelValues(append(append([]string(nil), v...), stage)...).Add(0)
			}
		}
		if m.consumerSeconds != nil {
			for _, state := range []string{bridge.RelayStateBusy, bridge.RelayStateIdle} {
				m.consumerSeconds.WithLabelValues(append(append([]string(nil), v...), state)...).Add(0)
			}
		}
		if m.producerSeconds != nil {
			for _, state := range []string{bridge.RelayStateBusy, bridge.RelayStateIdle} {
				m.producerSeconds.WithLabelValues(append(append([]string(nil), v...), state)...).Add(0)
			}
		}
	}
}

// IncMessages increments bifrost_relay_messages_total for this bridge.
func (m BridgeMetrics) IncMessages(id bridge.Identity) {
	if m.messages == nil {
		return
	}
	m.messages.WithLabelValues(id.LabelValues()...).Inc()
}

// IncErrors increments bifrost_relay_errors_total for this bridge and stage (poll, produce, commit, route).
func (m BridgeMetrics) IncErrors(id bridge.Identity, stage string) {
	if m.errors == nil {
		return
	}
	v := id.LabelValues()
	m.errors.WithLabelValues(append(append([]string(nil), v...), stage)...).Inc()
}

// ObserveProduceDuration records seconds for bifrost_relay_produce_duration_seconds (to-side produce path).
func (m BridgeMetrics) ObserveProduceDuration(id bridge.Identity, seconds float64) {
	if m.produceDuration == nil {
		return
	}
	m.produceDuration.WithLabelValues(id.LabelValues()...).Observe(seconds)
}

func (m BridgeMetrics) AddConsumerSeconds(id bridge.Identity, state string, seconds float64) {
	if m.consumerSeconds == nil || seconds <= 0 {
		return
	}
	v := id.LabelValues()
	m.consumerSeconds.WithLabelValues(append(append([]string(nil), v...), state)...).Add(seconds)
}

func (m BridgeMetrics) AddProducerSeconds(id bridge.Identity, state string, seconds float64) {
	if m.producerSeconds == nil || seconds <= 0 {
		return
	}
	v := id.LabelValues()
	m.producerSeconds.WithLabelValues(append(append([]string(nil), v...), state)...).Add(seconds)
}
