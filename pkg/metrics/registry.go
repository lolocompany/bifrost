package metrics

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"time"

	"github.com/lolocompany/bifrost/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
)

// MetricsRegistry holds process-level Prometheus collectors and the /metrics HTTP server lifecycle.
type MetricsRegistry struct {
	BridgeMetrics *BridgeMetrics
	BrokerMetrics *BrokerMetrics
	serverStop    func()
}

// StopServer shuts down the metrics HTTP listener if one was started.
func (m *MetricsRegistry) StopServer() {
	m.serverStop()
}

// NewFromConfig registers collectors from cfg and starts the /metrics HTTP server when enabled.
func NewFromConfig(cfg config.Config) (*MetricsRegistry, error) {
	reg := prometheus.NewRegistry()
	registerer := wrapRegistererWithExtraLabels(reg, cfg.Metrics.ExtraLabels)
	bridgeMetrics, brokerMetrics, err := New(registerer, cfg.Metrics, cfg.Bridges)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}

	stop, err := startMetricsHTTPServer(cfg.Metrics, reg)
	if err != nil {
		return nil, fmt.Errorf("metrics: %w", err)
	}

	return &MetricsRegistry{
		BridgeMetrics: bridgeMetrics,
		BrokerMetrics: brokerMetrics,
		serverStop:    stop,
	}, nil
}

func wrapRegistererWithExtraLabels(reg *prometheus.Registry, extraLabels map[string]string) prometheus.Registerer {
	if len(extraLabels) == 0 {
		return reg
	}
	labels := make(prometheus.Labels, len(extraLabels))
	for k, v := range extraLabels {
		labels[k] = v
	}
	return prometheus.WrapRegistererWith(labels, reg)
}

func startMetricsHTTPServer(cfg config.Metrics, reg *prometheus.Registry) (func(), error) {
	if !cfg.MetricsEnabled() {
		return func() {}, nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", Handler(reg))
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
	}

	ln, err := net.Listen("tcp", cfg.ListenAddr)
	if err != nil {
		return nil, fmt.Errorf("metrics listen: %w", err)
	}

	go func() {
		if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
			slog.Error("metrics server exited", "error_message", err.Error())
		}
	}()

	return func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := srv.Shutdown(shutdownCtx); err != nil {
			slog.Error("metrics shutdown", "error_message", err.Error())
		}
	}, nil
}
