package bifrost

import (
	"context"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
	"github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/twmb/franz-go/pkg/kgo"
)

func buildProducersByDestinationCluster(ctx context.Context, cfg config.Config, brokerMetrics *metrics.BrokerMetrics) (map[string]*kgo.Client, func(), error) {
	producers := make(map[string]*kgo.Client)

	for _, bridgeCfg := range cfg.Bridges {
		dest := bridgeCfg.To.Cluster
		if _, ok := producers[dest]; ok {
			continue
		}

		producer, err := newProducer(ctx, dest, cfg.Clusters[dest], brokerMetrics)
		if err != nil {
			closeProducerMap(producers)
			return nil, nil, fmt.Errorf("producer %q: %w", dest, err)
		}
		producers[dest] = producer
	}

	return producers, func() {
		closeProducerMap(producers)
	}, nil
}

func ensureTopicsForConfiguredBridges(ctx context.Context, cfg config.Config, producersByCluster map[string]*kgo.Client, brokerMetrics *metrics.BrokerMetrics) error {
	topicsPerCluster := topicsPerClusterFromBridges(cfg.Bridges)
	for _, clusterName := range sortedClusterNamesFromBridges(cfg.Bridges) {
		clusterCfg := cfg.Clusters[clusterName]
		if err := ensureTopicsForCluster(ctx, clusterCfg, clusterName, topicsPerCluster[clusterName], producersByCluster, brokerMetrics); err != nil {
			return err
		}
	}
	return nil
}

func newProducer(ctx context.Context, clusterName string, clusterCfg config.Cluster, brokerMetrics *metrics.BrokerMetrics) (*kgo.Client, error) {
	producer, err := kafka.NewProducer(&clusterCfg, kafkaClientHooks(brokerMetrics, clusterName), tcpDialRecorder(brokerMetrics, clusterName))
	if err != nil {
		return nil, err
	}
	logKafkaClientDebug(clusterName, "producer", &clusterCfg)

	pingCtx, cancelPing, err := kafka.WithPingTimeout(ctx, &clusterCfg)
	if err != nil {
		producer.Close()
		return nil, fmt.Errorf("ping: %w", err)
	}
	if err := kafka.PingBroker(pingCtx, producer); err != nil {
		cancelPing()
		producer.Close()
		return nil, fmt.Errorf("broker unreachable: %w", err)
	}
	cancelPing()

	slog.Debug("kafka cluster reachable", "cluster", clusterName, "client_role", "producer")
	return producer, nil
}

func newConsumer(ctx context.Context, bridgeCfg config.Bridge, fromCluster config.Cluster, brokerMetrics *metrics.BrokerMetrics) (*kgo.Client, error) {
	consumer, err := kafka.NewConsumerForBridge(
		&fromCluster,
		bridgeCfg.EffectiveConsumerGroup(),
		bridgeCfg.From.Topic,
		kafkaClientHooks(brokerMetrics, bridgeCfg.From.Cluster),
		tcpDialRecorder(brokerMetrics, bridgeCfg.From.Cluster),
	)
	if err != nil {
		return nil, fmt.Errorf("bridge %q consumer: %w", bridgeCfg.Name, err)
	}
	logKafkaClientDebug(bridgeCfg.From.Cluster, "consumer", &fromCluster)

	pingCtx, cancelPing, err := kafka.WithPingTimeout(ctx, &fromCluster)
	if err != nil {
		consumer.Close()
		return nil, fmt.Errorf("bridge %q consumer ping: %w", bridgeCfg.Name, err)
	}
	if err := kafka.PingBroker(pingCtx, consumer); err != nil {
		cancelPing()
		consumer.Close()
		return nil, fmt.Errorf("bridge %q from cluster %q: broker unreachable: %w", bridgeCfg.Name, bridgeCfg.From.Cluster, err)
	}
	cancelPing()

	slog.Debug("kafka cluster reachable", "cluster", bridgeCfg.From.Cluster, "bridge", bridgeCfg.Name, "client_role", "consumer")
	return consumer, nil
}

func ensureTopicsForCluster(
	ctx context.Context,
	clusterCfg config.Cluster,
	clusterName string,
	topics []string,
	producersByCluster map[string]*kgo.Client,
	brokerMetrics *metrics.BrokerMetrics,
) error {
	if !clusterCfg.AutoCreateTopics {
		return nil
	}

	client := producersByCluster[clusterName]
	closeAfter := false
	if client == nil {
		tmp, err := newProducer(ctx, clusterName, clusterCfg, brokerMetrics)
		if err != nil {
			return fmt.Errorf("cluster %q auto_create_topics: %w", clusterName, err)
		}
		client = tmp
		closeAfter = true
	}
	if closeAfter {
		defer client.Close()
	}

	created, err := kafka.EnsureTopics(ctx, client, clusterCfg.AutoCreateTopics, topics)
	if err != nil {
		return fmt.Errorf("cluster %q: ensure topics: %w", clusterName, err)
	}
	for _, topic := range created {
		slog.Info("kafka topic created", "cluster", clusterName, "topic", topic)
	}
	return nil
}

func sortedClusterNamesFromBridges(bridges []config.Bridge) []string {
	seen := make(map[string]struct{})
	for _, b := range bridges {
		seen[b.From.Cluster] = struct{}{}
		seen[b.To.Cluster] = struct{}{}
	}
	names := make([]string, 0, len(seen))
	for clusterName := range seen {
		names = append(names, clusterName)
	}
	sort.Strings(names)
	return names
}

func topicsPerClusterFromBridges(bridges []config.Bridge) map[string][]string {
	out := make(map[string][]string)
	for _, b := range bridges {
		out[b.From.Cluster] = append(out[b.From.Cluster], b.From.Topic)
		out[b.To.Cluster] = append(out[b.To.Cluster], b.To.Topic)
	}
	return out
}

func closeProducerMap(clients map[string]*kgo.Client) {
	for _, client := range clients {
		client.Close()
	}
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

// BridgeRunOptions builds bridge.RunOptions from validated cluster config.
func BridgeRunOptions(periodicStatsInterval time.Duration, fromCluster, toCluster config.Cluster) (bridge.RunOptions, error) {
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

// recordHeadersFromExtraHeaders builds Kafka headers from bridge extra_headers (sorted by key for stable output).
func recordHeadersFromExtraHeaders(m map[string]string) []kgo.RecordHeader {
	if len(m) == 0 {
		return nil
	}
	trimmed := make(map[string]string, len(m))
	for k, v := range m {
		trimmed[strings.TrimSpace(k)] = v
	}
	keys := make([]string, 0, len(trimmed))
	for k := range trimmed {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]kgo.RecordHeader, 0, len(keys))
	for _, k := range keys {
		out = append(out, kgo.RecordHeader{Key: k, Value: []byte(trimmed[k])})
	}
	return out
}

func logKafkaClientDebug(clusterName, clientRole string, env *config.Cluster) {
	if env == nil {
		return
	}
	attrs := []any{
		"cluster", clusterName,
		"client_role", clientRole,
		"seed_broker_count", len(env.Brokers),
		"tls_enabled", env.TLS.Enabled,
	}
	mech := strings.ToLower(strings.TrimSpace(env.SASL.Mechanism))
	if mech == "" {
		mech = "none"
	}
	attrs = append(attrs, "sasl_mechanism", mech)
	if id := strings.TrimSpace(env.Client.ClientID); id != "" {
		attrs = append(attrs, "client_id", id)
	}
	if d := strings.TrimSpace(env.Client.DialTimeout); d != "" {
		attrs = append(attrs, "client_dial_timeout", d)
	}
	if ro := strings.TrimSpace(env.Client.RequestTimeoutOverhead); ro != "" {
		attrs = append(attrs, "client_request_timeout_overhead", ro)
	}

	switch clientRole {
	case "consumer":
		il := strings.ToLower(strings.TrimSpace(env.Consumer.IsolationLevel))
		if il == "" {
			il = "read_uncommitted"
		}
		attrs = append(attrs, "consumer_isolation_level", il)
		if env.Consumer.FetchMaxBytes != nil {
			attrs = append(attrs, "consumer_fetch_max_bytes", *env.Consumer.FetchMaxBytes)
		}
		if env.Consumer.FetchMaxPartitionBytes != nil {
			attrs = append(attrs, "consumer_fetch_max_partition_bytes", *env.Consumer.FetchMaxPartitionBytes)
		}
		if w := strings.TrimSpace(env.Consumer.FetchMaxWait); w != "" {
			attrs = append(attrs, "consumer_fetch_max_wait", w)
		}
	case "producer":
		acks := strings.ToLower(strings.TrimSpace(env.Producer.RequiredAcks))
		if acks == "" {
			acks = "all"
		}
		attrs = append(attrs, "producer_required_acks", acks)
		if c := strings.TrimSpace(env.Producer.BatchCompression); c != "" {
			attrs = append(attrs, "producer_batch_compression", c)
		}
		if env.Producer.BatchMaxBytes != nil {
			attrs = append(attrs, "producer_batch_max_bytes", *env.Producer.BatchMaxBytes)
		}
		if l := strings.TrimSpace(env.Producer.Linger); l != "" {
			attrs = append(attrs, "producer_linger", l)
		}
	}

	slog.Debug("kafka client config", attrs...)
}
