// Package run wires config into Kafka clients, metrics, and bridge relays.
package run

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"sort"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
	"github.com/lolocompany/bifrost/pkg/config"
	bifrostkafka "github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"
)

// Run starts metrics (if enabled), producer clients, optional topic creation, and bridge goroutines.
func Run(ctx context.Context, cfg *config.Config) error {
	periodicStatsInterval, err := cfg.Logging.ParsePeriodicStatsInterval()
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}

	reg := prometheus.NewRegistry()
	var reger prometheus.Registerer = reg
	if len(cfg.Metrics.ExtraLabels) > 0 {
		labels := prometheus.Labels{}
		for k, v := range cfg.Metrics.ExtraLabels {
			labels[k] = v
		}
		reger = prometheus.WrapRegistererWith(labels, reger)
	}
	m, brokerMetrics, err := metrics.New(reger, cfg.Metrics, cfg.Bridges)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
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

	if cfg.Metrics.MetricsEnabled() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(reg))
		srv := &http.Server{
			Handler:           mux,
			ReadHeaderTimeout: 10 * time.Second,
		}

		ln, err := net.Listen("tcp", cfg.Metrics.ListenAddr)
		if err != nil {
			return fmt.Errorf("metrics listen: %w", err)
		}

		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server exited", "error_message", err.Error())
			}
		}()

		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				slog.Error("metrics shutdown", "error_message", err.Error())
			}
		}()
	}

	producerClients := make(map[string]*kgo.Client)
	defer func() {
		for _, cl := range producerClients {
			cl.Close()
		}
	}()

	topicsByCluster := make(map[string][]string)
	for _, br := range cfg.Bridges {
		topicsByCluster[br.From.Cluster] = append(topicsByCluster[br.From.Cluster], br.From.Topic)
		topicsByCluster[br.To.Cluster] = append(topicsByCluster[br.To.Cluster], br.To.Topic)
	}

	for _, br := range cfg.Bridges {
		clusterName := br.To.Cluster
		if _, ok := producerClients[clusterName]; ok {
			continue
		}
		toCluster := cfg.Clusters[clusterName]
		p, err := bifrostkafka.NewProducer(&toCluster, kafkaClientHooks(brokerMetrics, clusterName), tcpDialRecorder(brokerMetrics, clusterName))
		if err != nil {
			return fmt.Errorf("producer %q: %w", clusterName, err)
		}
		logKafkaClientDebug(clusterName, "producer", &toCluster)
		pingCtx, cancelPing, err := bifrostkafka.WithPingTimeout(ctx, &toCluster)
		if err != nil {
			p.Close()
			return fmt.Errorf("producer %q ping: %w", clusterName, err)
		}
		if err := bifrostkafka.PingBroker(pingCtx, p); err != nil {
			cancelPing()
			p.Close()
			return fmt.Errorf("producer %q: broker unreachable: %w", clusterName, err)
		}
		cancelPing()
		slog.Debug("kafka cluster reachable", "cluster", clusterName, "client_role", "producer")
		producerClients[clusterName] = p
	}

	for _, clusterName := range bridgeClusterNames(cfg.Bridges) {
		clusterCfg := cfg.Clusters[clusterName]
		if err := ensureTopicsForCluster(ctx, clusterCfg, clusterName, topicsByCluster[clusterName], producerClients, brokerMetrics); err != nil {
			return err
		}
	}

	g, gctx := errgroup.WithContext(ctx)

	for _, br := range cfg.Bridges {
		g.Go(func() error {
			fromCluster := cfg.Clusters[br.From.Cluster]
			runOpts, err := bridgeRunOptions(periodicStatsInterval, fromCluster, cfg.Clusters[br.To.Cluster])
			if err != nil {
				return fmt.Errorf("bridge %q retry config: %w", br.Name, err)
			}
			consumer, err := bifrostkafka.NewConsumerForBridge(
				&fromCluster,
				br.EffectiveConsumerGroup(),
				br.From.Topic,
				kafkaClientHooks(brokerMetrics, br.From.Cluster),
				tcpDialRecorder(brokerMetrics, br.From.Cluster),
			)
			if err != nil {
				return fmt.Errorf("bridge %q consumer: %w", br.Name, err)
			}
			defer consumer.Close()
			logKafkaClientDebug(br.From.Cluster, "consumer", &fromCluster)

			pingCtx, cancelPing, err := bifrostkafka.WithPingTimeout(gctx, &fromCluster)
			if err != nil {
				return fmt.Errorf("bridge %q consumer ping: %w", br.Name, err)
			}
			if err := bifrostkafka.PingBroker(pingCtx, consumer); err != nil {
				cancelPing()
				return fmt.Errorf("bridge %q from cluster %q: broker unreachable: %w", br.Name, br.From.Cluster, err)
			}
			cancelPing()
			slog.Debug("kafka cluster reachable", "cluster", br.From.Cluster, "bridge", br.Name, "client_role", "consumer")

			producer := producerClients[br.To.Cluster]
			slog.Info("bridge starting",
				"bridge", br.Name,
				"consumer_group", br.EffectiveConsumerGroup(),
				"from_cluster", br.From.Cluster,
				"to_cluster", br.To.Cluster,
			)
			if err := bridge.Run(gctx, bridge.IdentityFrom(br), consumer, producer, m, runOpts); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("bridge %q: %w", br.Name, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

// ensureTopicsForCluster creates missing from/to topics on this cluster only when clusterCfg.AutoCreateTopics is true.
func ensureTopicsForCluster(
	ctx context.Context,
	clusterCfg config.Cluster,
	clusterName string,
	topics []string,
	producerClients map[string]*kgo.Client,
	brokerMetrics *metrics.BrokerMetrics,
) error {
	if !clusterCfg.AutoCreateTopics {
		return nil
	}
	var cl *kgo.Client
	closeClient := false
	if p, ok := producerClients[clusterName]; ok {
		cl = p
	} else {
		tmp, err := bifrostkafka.NewProducer(&clusterCfg, kafkaClientHooks(brokerMetrics, clusterName), tcpDialRecorder(brokerMetrics, clusterName))
		if err != nil {
			return fmt.Errorf("cluster %q auto_create_topics: %w", clusterName, err)
		}
		logKafkaClientDebug(clusterName, "producer", &clusterCfg)
		pingCtx, cancelPing, err := bifrostkafka.WithPingTimeout(ctx, &clusterCfg)
		if err != nil {
			tmp.Close()
			return fmt.Errorf("cluster %q auto_create_topics ping: %w", clusterName, err)
		}
		if err := bifrostkafka.PingBroker(pingCtx, tmp); err != nil {
			cancelPing()
			tmp.Close()
			return fmt.Errorf("cluster %q auto_create_topics: broker unreachable: %w", clusterName, err)
		}
		cancelPing()
		cl = tmp
		closeClient = true
	}
	created, err := bifrostkafka.EnsureTopics(ctx, cl, clusterCfg.AutoCreateTopics, topics)
	if closeClient {
		cl.Close()
	}
	if err != nil {
		return fmt.Errorf("cluster %q: ensure topics: %w", clusterName, err)
	}
	for _, topic := range created {
		slog.Info("kafka topic created", "cluster", clusterName, "topic", topic)
	}
	return nil
}

// bridgeClusterNames returns sorted unique cluster names referenced by bridges.
func bridgeClusterNames(bridges []config.Bridge) []string {
	seen := make(map[string]struct{})
	for _, br := range bridges {
		seen[br.From.Cluster] = struct{}{}
		seen[br.To.Cluster] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for c := range seen {
		out = append(out, c)
	}
	sort.Strings(out)
	return out
}

func kafkaClientHooks(broker *metrics.BrokerMetrics, cluster string) []kgo.Hook {
	if broker == nil {
		return nil
	}
	h := broker.HookFor(cluster)
	if h == nil {
		return nil
	}
	return []kgo.Hook{h}
}

func tcpDialRecorder(broker *metrics.BrokerMetrics, cluster string) func(float64) {
	if broker == nil {
		return nil
	}
	return broker.TCPDialRecorder(cluster)
}

func bridgeRunOptions(periodicStatsInterval time.Duration, fromCluster, toCluster config.Cluster) (bridge.RunOptions, error) {
	commitRetry, err := fromCluster.Consumer.CommitRetry.Durations("consumer.commit_retry", config.DefaultCommitRetry)
	if err != nil {
		return bridge.RunOptions{}, err
	}
	produceRetry, err := toCluster.Producer.Retry.Durations("producer.retry", config.DefaultProducerRetry)
	if err != nil {
		return bridge.RunOptions{}, err
	}
	return bridge.RunOptions{
		PeriodicStatsInterval: periodicStatsInterval,
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{
				MinBackoff: produceRetry.MinBackoff,
				MaxBackoff: produceRetry.MaxBackoff,
				Jitter:     produceRetry.Jitter,
			},
			Commit: bridge.RetryConfig{
				MinBackoff: commitRetry.MinBackoff,
				MaxBackoff: commitRetry.MaxBackoff,
				Jitter:     commitRetry.Jitter,
			},
		},
	}, nil
}
