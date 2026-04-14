// Package bifrost orchestrates all configured bridges in one OS process (metrics, Kafka clients,
// topic ensure, concurrent bridge.Run). Single-bridge relay behavior lives in pkg/bridge.
package bifrost

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lolocompany/bifrost/pkg/bridge"
	"github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"golang.org/x/sync/errgroup"
)

// Run starts metrics (if enabled), producer clients, optional topic creation, and bridge goroutines.
// Does not configure logging. Use pkg/logging.Setup before calling Run to set up the default slog logger.
func Run(ctx context.Context, cfg config.Config) error {
	periodicStatsInterval, err := cfg.Logging.ParsePeriodicStatsInterval()
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}

	startupAttrs := []any{
		"bridge_count", len(cfg.Bridges),
		"periodic_stats_interval", periodicStatsInterval.String(),
		"metrics_enabled", cfg.Metrics.MetricsEnabled(),
	}
	if cfg.Metrics.MetricsEnabled() {
		startupAttrs = append(startupAttrs, "metrics_listen_addr", cfg.Metrics.ListenAddr)
	}
	slog.Debug("run startup", startupAttrs...)

	metricsRegistry, err := metrics.NewFromConfig(cfg)
	if err != nil {
		return err
	}
	defer metricsRegistry.StopServer()

	for _, br := range cfg.Bridges {
		slog.Debug("bridge wiring",
			"bridge", br.Name,
			"consumer_group", br.EffectiveConsumerGroup(),
			"from_cluster", br.From.Cluster,
			"from_topic", br.From.Topic,
			"to_cluster", br.To.Cluster,
			"to_topic", br.To.Topic,
		)
	}

	producersByCluster, closeProducers, err := buildProducersByDestinationCluster(ctx, cfg, metricsRegistry.BrokerMetrics)
	if err != nil {
		return err
	}
	defer closeProducers()

	if err := ensureTopicsForConfiguredBridges(ctx, cfg, producersByCluster, metricsRegistry.BrokerMetrics); err != nil {
		return err
	}

	eg, ctx := errgroup.WithContext(ctx)
	for _, bridgeCfg := range cfg.Bridges {
		eg.Go(func() error {
			fromCluster := cfg.Clusters[bridgeCfg.From.Cluster]

			runOpts, err := BridgeRunOptions(periodicStatsInterval, fromCluster, cfg.Clusters[bridgeCfg.To.Cluster])
			if err != nil {
				return fmt.Errorf("bridge %q retry config: %w", bridgeCfg.Name, err)
			}
			runOpts.ExtraHeaders = recordHeadersFromExtraHeaders(bridgeCfg.ExtraHeaders)

			consumer, err := newConsumer(ctx, bridgeCfg, fromCluster, metricsRegistry.BrokerMetrics)
			if err != nil {
				return err
			}
			defer consumer.Close()

			producer := producersByCluster[bridgeCfg.To.Cluster]
			slog.Info("bridge starting",
				"bridge", bridgeCfg.Name,
				"consumer_group", bridgeCfg.EffectiveConsumerGroup(),
				"from_cluster", bridgeCfg.From.Cluster,
				"to_cluster", bridgeCfg.To.Cluster,
			)
			if err := bridge.Run(ctx, bridge.IdentityFrom(bridgeCfg), consumer, producer, metricsRegistry.BridgeMetrics, runOpts); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("bridge %q: %w", bridgeCfg.Name, err)
			}
			return nil
		})
	}
	return eg.Wait()
}
