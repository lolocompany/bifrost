// Package metrics registers Prometheus collectors for bifrost and can start the /metrics HTTP server.
//
// Use [NewFromConfig] for the full process setup (registry, collectors, listener). Use [New] when
// you already have a [prometheus.Registerer].
//
// Naming follows Prometheus and OpenMetrics conventions (counters end with _total,
// duration histograms use _duration_seconds, etc.). Application metrics use the bifrost_
// prefix; Go runtime and process collectors keep standard go_* and process_* names.
package metrics

import (
	"errors"
	"net/http"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// New registers per-bridge metrics, broker-scoped metrics, and runtime collectors when enabled.
// bridges is used to pre-register per-bridge label combinations (zero counters) so all bridges appear on /metrics.
func New(reg prometheus.Registerer, cfg bifrostconfig.Metrics, bridges []bifrostconfig.Bridge) (*BridgeMetrics, *BrokerMetrics, error) {
	if reg == nil {
		return nil, nil, errors.New("registerer is nil")
	}
	if !cfg.MetricsEnabled() {
		return &BridgeMetrics{}, nil, nil
	}

	if err := RegisterRuntimeCollectors(reg, cfg.Groups); err != nil {
		return nil, nil, err
	}
	broker, err := newBrokerMetrics(reg, cfg.Groups)
	if err != nil {
		return nil, nil, err
	}

	bridge, err := newBridgeMetrics(reg, bridges)
	if err != nil {
		return nil, nil, err
	}
	return bridge, broker, nil
}

// Handler returns an HTTP handler that exposes metrics from reg.
func Handler(reg prometheus.Gatherer) http.Handler {
	return promhttp.HandlerFor(reg, promhttp.HandlerOpts{})
}
