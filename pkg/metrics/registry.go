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
	"github.com/prometheus/client_golang/prometheus/promhttp"
	dto "github.com/prometheus/client_model/go"
)

// MetricsRegistry holds process-level Prometheus collectors and the /metrics HTTP server lifecycle.
type MetricsRegistry struct {
	BridgeMetrics BridgeMetrics
	BrokerMetrics BrokerMetrics
	gatherer      prometheus.Gatherer
	serverStop    func()
}

// Gather returns the current set of metric families (Prometheus text exposition source).
func (m *MetricsRegistry) Gather() ([]*dto.MetricFamily, error) {
	if m.gatherer == nil {
		return nil, nil
	}
	return m.gatherer.Gather()
}

// StopServer shuts down the metrics HTTP listener if one was started.
func (m *MetricsRegistry) StopServer() {
	m.serverStop()
}

// NewFromConfig registers collectors from cfg and starts the /metrics HTTP server when enabled.
func NewFromConfig(cfg config.Config) (MetricsRegistry, error) {
	reg := prometheus.NewRegistry()
	registerer := wrapRegistererWithExtraLabels(reg, cfg.Metrics.ExtraLabels)
	m := cfg.Metrics

	var bridgeMetrics BridgeMetrics
	var brokerMetrics BrokerMetrics
	if !m.MetricsEnabled() {
		bridgeMetrics = BridgeMetrics{}
	} else {
		if err := RegisterRuntimeCollectors(registerer, m.Groups); err != nil {
			return MetricsRegistry{}, fmt.Errorf("metrics: %w", err)
		}
		var err error
		brokerMetrics, err = newBrokerMetrics(registerer, m.Groups)
		if err != nil {
			return MetricsRegistry{}, fmt.Errorf("metrics: %w", err)
		}
		bridgeMetrics, err = newBridgeMetrics(registerer, cfg.Bridges)
		if err != nil {
			return MetricsRegistry{}, fmt.Errorf("metrics: %w", err)
		}
	}

	stop, err := startMetricsHTTPServer(cfg.Metrics, reg)
	if err != nil {
		return MetricsRegistry{}, fmt.Errorf("metrics: %w", err)
	}

	return MetricsRegistry{
		BridgeMetrics: bridgeMetrics,
		BrokerMetrics: brokerMetrics,
		gatherer:      reg,
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

type errorLogger struct {
	logger *slog.Logger
}

func (l *errorLogger) Println(v ...interface{}) {
	l.logger.Error(fmt.Sprint(v...))
}

func startMetricsHTTPServer(cfg config.Metrics, reg *prometheus.Registry) (func(), error) {
	if !cfg.MetricsEnabled() {
		return func() {}, nil
	}

	mux := http.NewServeMux()
	mux.Handle("/metrics", promhttp.HandlerFor(reg, promhttp.HandlerOpts{
		ErrorLog:          &errorLogger{logger: slog.Default()},
		ErrorHandling:     promhttp.HTTPErrorOnError,
		Timeout:           10 * time.Second,
		EnableOpenMetrics: true,
	}))
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
