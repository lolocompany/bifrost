// Package app composes Bifrost's process-level dependencies and starts configured relays.
package app

import (
	"context"
	"errors"
	"fmt"
	"log/slog"

	"github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/domain/relay"
	"github.com/lolocompany/bifrost/internal/observability/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// Run starts metrics (if enabled), producer clients, optional topic creation, and bridge goroutines.
// Does not configure logging. Use observability/logging.Setup before calling Run to set up the default slog logger.
func Run(ctx context.Context, cfg config.Config) error {
	cfg.ApplyDefaults()
	if err := cfg.Validate(); err != nil {
		return fmt.Errorf("config: %w", err)
	}

	periodicStatsInterval, err := cfg.Logging.ParsePeriodicStatsInterval()
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}

	metricsRegistry, err := metrics.NewFromConfig(cfg)
	if err != nil {
		return err
	}
	defer metricsRegistry.StopServer()

	producersByCluster, closeProducers, err := buildProducersByDestinationCluster(ctx, cfg, metricsRegistry.BrokerMetrics)
	if err != nil {
		return err
	}
	defer closeProducers()

	if err := ensureTopicsForConfiguredBridges(ctx, cfg, producersByCluster, metricsRegistry.BrokerMetrics); err != nil {
		return err
	}

	snap := probeSystemSnapshot()
	configReplicas, partitions, err := collectReplicaInputs(ctx, cfg, producersByCluster, metricsRegistry.BrokerMetrics)
	if err != nil {
		return err
	}

	effReplicas, plan, err := PlanReplicaCounts(configReplicas, partitions, snap)
	if err != nil {
		return err
	}

	autoBridgeCount := 0
	for _, r := range configReplicas {
		if r == 0 {
			autoBridgeCount++
		}
	}

	if plan.ExplicitReplicaSum > plan.GlobalSoftCap {
		slog.Warn("configured bridge replicas exceed soft global relay cap",
			"explicit_replica_sum", plan.ExplicitReplicaSum,
			"global_soft_cap", plan.GlobalSoftCap,
		)
	}
	if autoBridgeCount > 0 && plan.HeadroomForAuto < autoBridgeCount {
		slog.Warn("not enough relay headroom for automatic replica targets; using one relay per auto-sized bridge",
			"auto_bridges", autoBridgeCount,
			"headroom", plan.HeadroomForAuto,
		)
	} else if autoBridgeCount > 0 && plan.HeadroomForAuto >= autoBridgeCount && plan.AutoRawTargetSum > plan.HeadroomForAuto {
		slog.Info("replica autoscale applied global fair-share",
			"auto_raw_target_sum", plan.AutoRawTargetSum,
			"headroom", plan.HeadroomForAuto,
			"gomaxprocs", snap.GOMAXPROCS,
		)
	}

	bridgeWorkers := 0
	for _, n := range effReplicas {
		bridgeWorkers += n
	}

	startupAttrs := []any{
		"bridge_count", len(cfg.Bridges),
		"bridge_worker_count", bridgeWorkers,
		"relay_global_soft_cap", plan.GlobalSoftCap,
		"periodic_stats_interval", periodicStatsInterval.String(),
		"metrics_enabled", cfg.Metrics.MetricsEnabled(),
	}
	if snap.AvailableBytes > 0 {
		startupAttrs = append(startupAttrs, "memory_available_bytes", snap.AvailableBytes)
	}
	if cfg.Metrics.MetricsEnabled() {
		startupAttrs = append(startupAttrs, "metrics_listen_addr", cfg.Metrics.ListenAddr)
	}
	slog.Debug("run startup", startupAttrs...)

	for i, br := range cfg.Bridges {
		slog.Debug("bridge wiring",
			"bridge", br.Name,
			"replicas", effReplicas[i],
			"replicas_config", br.Replicas,
			"consumer_group", br.EffectiveConsumerGroup(),
			"from_cluster", br.From.Cluster,
			"from_topic", br.From.Topic,
			"to_cluster", br.To.Cluster,
			"to_topic", br.To.Topic,
		)
	}

	eg, ctx := errgroup.WithContext(ctx)
	if serverErr := metricsRegistry.ServerErr(); serverErr != nil {
		eg.Go(func() error {
			select {
			case err := <-serverErr:
				if err == nil {
					return nil
				}
				return fmt.Errorf("metrics server exited: %w", err)
			case <-ctx.Done():
				return nil
			}
		})
	}
	for i, bridgeCfg := range cfg.Bridges {
		n := effReplicas[i]
		for replica := range n {
			bridgeCfg := bridgeCfg
			replica := replica
			eg.Go(func() error {
				fromCluster := cfg.Clusters[bridgeCfg.From.Cluster]

				runOpts, err := RelayOptionsFromBridge(periodicStatsInterval, bridgeCfg, fromCluster, cfg.Clusters[bridgeCfg.To.Cluster])
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
					"replica", replica,
					"replicas", n,
					"replicas_config", bridgeCfg.Replicas,
					"consumer_group", bridgeCfg.EffectiveConsumerGroup(),
					"from_cluster", bridgeCfg.From.Cluster,
					"to_cluster", bridgeCfg.To.Cluster,
				)
				if err := relay.Run(ctx, relayIdentityFromBridge(bridgeCfg), consumer, producer, metricsRegistry.BridgeMetrics, runOpts); err != nil {
					if errors.Is(err, context.Canceled) {
						return nil
					}
					return fmt.Errorf("bridge %q replica %d: %w", bridgeCfg.Name, replica, err)
				}
				return nil
			})
		}
	}
	return eg.Wait()
}

func collectReplicaInputs(
	ctx context.Context,
	cfg config.Config,
	producersByCluster map[string]*kgo.Client,
	brokerMetrics metrics.BrokerMetrics,
) ([]int, []int, error) {
	configReplicas := make([]int, len(cfg.Bridges))
	partitions := make([]int, len(cfg.Bridges))
	sourcePartitionsByBridge := make(map[string]int, len(cfg.Bridges))
	for i, bridgeCfg := range cfg.Bridges {
		fromCluster := cfg.Clusters[bridgeCfg.From.Cluster]
		sourcePartitions, ok := sourcePartitionsByBridge[bridgeCfg.Name]
		if !ok {
			n, err := sourceTopicPartitionCount(ctx, bridgeCfg, fromCluster, producersByCluster, brokerMetrics)
			if err != nil {
				return nil, nil, fmt.Errorf("bridge %q source topic partitions: %w", bridgeCfg.Name, err)
			}
			sourcePartitions = n
			sourcePartitionsByBridge[bridgeCfg.Name] = n
		}
		if bridgeCfg.PartitionsPreserved() {
			destPartitions, err := topicPartitionCount(ctx, bridgeCfg.To.Cluster, bridgeCfg.To.Topic, cfg.Clusters[bridgeCfg.To.Cluster], producersByCluster, brokerMetrics)
			if err != nil {
				return nil, nil, fmt.Errorf("bridge %q destination topic partitions: %w", bridgeCfg.Name, err)
			}
			if err := ValidatePreservedPartitionCounts(bridgeCfg, sourcePartitions, destPartitions); err != nil {
				return nil, nil, err
			}
		}
		configReplicas[i] = bridgeCfg.Replicas
		if bridgeCfg.Replicas == 0 {
			partitions[i] = sourcePartitions
		}
	}
	return configReplicas, partitions, nil
}

// ValidatePreservedPartitionCounts checks whether a bridge can preserve source partition IDs on
// its destination topic or force records onto a configured destination partition.
func ValidatePreservedPartitionCounts(bridgeCfg config.Bridge, sourcePartitions, destPartitions int) error {
	if overridePartition, ok := bridgeCfg.EffectiveOverridePartition(); ok {
		if int(overridePartition) < destPartitions {
			return nil
		}
		return fmt.Errorf(
			"bridge %q override_partition=%d requires destination topic %q on cluster %q to have at least %d partitions (destination has %d)",
			bridgeCfg.Name,
			overridePartition,
			bridgeCfg.To.Topic,
			bridgeCfg.To.Cluster,
			int(overridePartition)+1,
			destPartitions,
		)
	}
	if !bridgeCfg.PartitionsPreserved() {
		return nil
	}
	if destPartitions >= sourcePartitions {
		return nil
	}
	return fmt.Errorf(
		"bridge %q requires destination topic %q on cluster %q to have at least %d partitions to preserve source partitions (source %q on cluster %q has %d, destination has %d)",
		bridgeCfg.Name,
		bridgeCfg.To.Topic,
		bridgeCfg.To.Cluster,
		sourcePartitions,
		bridgeCfg.From.Topic,
		bridgeCfg.From.Cluster,
		sourcePartitions,
		destPartitions,
	)
}
