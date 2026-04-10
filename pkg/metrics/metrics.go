// Package metrics registers Prometheus collectors for the bridge.
package metrics

import (
	"errors"
	"fmt"
	"net/http"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Metrics holds Prometheus collectors used by the bridge.
type Metrics struct {
	forwarded      *prometheus.CounterVec
	forwardErrors  *prometheus.CounterVec
	forwardLatency *prometheus.HistogramVec
}

// New registers bridge forwarding collectors and platform collectors when metrics are enabled.
// bridges is used to pre-register per-bridge label combinations (zero counters) so all bridges appear on /metrics.
func New(reg prometheus.Registerer, cfg bifrostconfig.Metrics, bridges []bifrostconfig.Bridge) (*Metrics, *BrokerProm, error) {
	if reg == nil {
		return nil, nil, errors.New("registerer is nil")
	}
	if !cfg.MetricsEnabled() {
		return &Metrics{}, nil, nil
	}

	if err := RegisterPlatformCollectors(reg, cfg.Groups); err != nil {
		return nil, nil, err
	}
	bp, err := NewBrokerProm(reg, cfg.Groups)
	if err != nil {
		return nil, nil, err
	}

	m := &Metrics{}
	g := cfg.Groups

	if g.GroupForward() {
		c := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_relay_messages_total",
				Help: "Total number of messages successfully produced on the to-side cluster.",
			},
			BridgeLabelNames,
		)
		if err := reg.Register(c); err != nil {
			return nil, nil, fmt.Errorf("register forward counter: %w", err)
		}
		m.forwarded = c
	}

	if g.GroupErrors() {
		cv := prometheus.NewCounterVec(
			prometheus.CounterOpts{
				Name: "bifrost_relay_errors_total",
				Help: "Total number of relay errors by stage (poll, produce, commit, route).",
			},
			append(append([]string(nil), BridgeLabelNames...), "stage"),
		)
		if err := reg.Register(cv); err != nil {
			return nil, nil, fmt.Errorf("register errors counter: %w", err)
		}
		m.forwardErrors = cv
	}

	if g.GroupLatency() {
		h := prometheus.NewHistogramVec(
			prometheus.HistogramOpts{
				Name:    "bifrost_relay_produce_duration_seconds",
				Help:    "Wall-clock duration in seconds to produce a relayed record on the to-side cluster.",
				Buckets: prometheus.DefBuckets,
			},
			BridgeLabelNames,
		)
		if err := reg.Register(h); err != nil {
			return nil, nil, fmt.Errorf("register latency histogram: %w", err)
		}
		m.forwardLatency = h
	}

	initBridgeSeries(m, bridges)

	return m, bp, nil
}

func initBridgeSeries(m *Metrics, bridges []bifrostconfig.Bridge) {
	for _, br := range bridges {
		id := BridgeIdentityFrom(br)
		v := id.LabelValues()
		if m.forwarded != nil {
			m.forwarded.WithLabelValues(v...).Add(0)
		}
		if m.forwardErrors != nil {
			for _, stage := range []string{"poll", "produce", "commit", "route"} {
				m.forwardErrors.WithLabelValues(append(append([]string(nil), v...), stage)...).Add(0)
			}
		}
	}
}

// AddForwarded increments successful relay count for this bridge.
func (m *Metrics) AddForwarded(id BridgeIdentity) {
	if m == nil || m.forwarded == nil {
		return
	}
	m.forwarded.WithLabelValues(id.LabelValues()...).Inc()
}

// AddForwardError records a failure with a low-cardinality stage label (e.g. produce, poll).
func (m *Metrics) AddForwardError(id BridgeIdentity, stage string) {
	if m == nil || m.forwardErrors == nil {
		return
	}
	v := id.LabelValues()
	m.forwardErrors.WithLabelValues(append(append([]string(nil), v...), stage)...).Inc()
}

// ObserveForwardLatency records seconds spent producing on the to side for this bridge.
func (m *Metrics) ObserveForwardLatency(id BridgeIdentity, seconds float64) {
	if m == nil || m.forwardLatency == nil {
		return
	}
	m.forwardLatency.WithLabelValues(id.LabelValues()...).Observe(seconds)
}

// Handler returns an HTTP handler that exposes metrics from reg.
func Handler(reg prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
