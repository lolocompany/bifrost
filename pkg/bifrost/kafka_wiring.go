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

func buildProducersByDestinationCluster(ctx context.Context, cfg config.Config, brokerMetrics metrics.BrokerMetrics) (map[string]*kgo.Client, func(), error) {
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

func ensureTopicsForConfiguredBridges(ctx context.Context, cfg config.Config, producersByCluster map[string]*kgo.Client, brokerMetrics metrics.BrokerMetrics) error {
	topicsPerCluster := topicsPerClusterFromBridges(cfg.Bridges)
	for _, clusterName := range sortedClusterNamesFromBridges(cfg.Bridges) {
		clusterCfg := cfg.Clusters[clusterName]
		if err := ensureTopicsForCluster(ctx, clusterCfg, clusterName, topicsPerCluster[clusterName], producersByCluster, brokerMetrics); err != nil {
			return err
		}
	}
	return nil
}

func newProducer(ctx context.Context, clusterName string, clusterCfg config.Cluster, brokerMetrics metrics.BrokerMetrics) (*kgo.Client, error) {
	producer, err := kafka.NewProducer(&clusterCfg, kafkaClientHooks(brokerMetrics, clusterName), tcpDialRecorder(brokerMetrics, clusterName))
	if err != nil {
		return nil, err
	}
	logKafkaClientDebug(clusterName, "producer", &clusterCfg)
	if err := pingKafkaClient(ctx, producer, &clusterCfg); err != nil {
		producer.Close()
		return nil, fmt.Errorf("broker unreachable: %w", err)
	}

	slog.Debug("kafka cluster reachable", "cluster", clusterName, "client_role", "producer")
	return producer, nil
}

func newConsumer(ctx context.Context, bridgeCfg config.Bridge, fromCluster config.Cluster, brokerMetrics metrics.BrokerMetrics) (*kgo.Client, error) {
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
	if err := pingKafkaClient(ctx, consumer, &fromCluster); err != nil {
		consumer.Close()
		return nil, fmt.Errorf("bridge %q from cluster %q: broker unreachable: %w", bridgeCfg.Name, bridgeCfg.From.Cluster, err)
	}

	slog.Debug("kafka cluster reachable", "cluster", bridgeCfg.From.Cluster, "bridge", bridgeCfg.Name, "client_role", "consumer")
	return consumer, nil
}

func topicPartitionCount(ctx context.Context, clusterName, topic string, clusterCfg config.Cluster, producersByCluster map[string]*kgo.Client, brokerMetrics metrics.BrokerMetrics) (int, error) {
	client, closeClient, err := resolveProducerForCluster(ctx, clusterName, clusterCfg, producersByCluster, brokerMetrics)
	if err != nil {
		return 0, fmt.Errorf("cluster %q: %w", clusterName, err)
	}
	defer closeClient()
	n, err := kafka.TopicPartitionCount(ctx, client, topic)
	if err != nil {
		return 0, fmt.Errorf("topic %q on cluster %q: %w", topic, clusterName, err)
	}
	return n, nil
}

func sourceTopicPartitionCount(ctx context.Context, bridgeCfg config.Bridge, fromClusterCfg config.Cluster, producersByCluster map[string]*kgo.Client, brokerMetrics metrics.BrokerMetrics) (int, error) {
	return topicPartitionCount(ctx, bridgeCfg.From.Cluster, bridgeCfg.From.Topic, fromClusterCfg, producersByCluster, brokerMetrics)
}

func ensureTopicsForCluster(
	ctx context.Context,
	clusterCfg config.Cluster,
	clusterName string,
	topics []string,
	producersByCluster map[string]*kgo.Client,
	brokerMetrics metrics.BrokerMetrics,
) error {
	if !clusterCfg.AutoCreateTopics {
		return nil
	}
	client, closeClient, err := resolveProducerForCluster(ctx, clusterName, clusterCfg, producersByCluster, brokerMetrics)
	if err != nil {
		return fmt.Errorf("cluster %q auto_create_topics: %w", clusterName, err)
	}
	defer closeClient()

	created, err := kafka.EnsureTopics(ctx, client, true, topics)
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

func kafkaClientHooks(broker metrics.BrokerMetrics, cluster string) []kgo.Hook {
	h := broker.HookFor(cluster)
	if h == nil {
		return nil
	}
	return []kgo.Hook{h}
}

func tcpDialRecorder(broker metrics.BrokerMetrics, cluster string) func(float64) {
	return broker.TCPDialRecorder(cluster)
}

func pingKafkaClient(ctx context.Context, client *kgo.Client, clusterCfg *config.Cluster) error {
	pingCtx, cancelPing, err := kafka.WithPingTimeout(ctx, clusterCfg)
	if err != nil {
		return fmt.Errorf("ping timeout: %w", err)
	}
	defer cancelPing()
	return kafka.PingBroker(pingCtx, client)
}

func resolveProducerForCluster(
	ctx context.Context,
	clusterName string,
	clusterCfg config.Cluster,
	producersByCluster map[string]*kgo.Client,
	brokerMetrics metrics.BrokerMetrics,
) (*kgo.Client, func(), error) {
	if client := producersByCluster[clusterName]; client != nil {
		return client, func() {}, nil
	}
	client, err := newProducer(ctx, clusterName, clusterCfg, brokerMetrics)
	if err != nil {
		return nil, nil, err
	}
	return client, func() { client.Close() }, nil
}

// BridgeRunOptions builds bridge.RunOptions from validated bridge and cluster config.
func BridgeRunOptions(periodicStatsInterval time.Duration, bridgeCfg config.Bridge, fromCluster, toCluster config.Cluster) (bridge.RunOptions, error) {
	commitRetry, err := fromCluster.Consumer.CommitRetry.Durations("consumer.commit_retry", config.DefaultCommitRetry)
	if err != nil {
		return bridge.RunOptions{}, err
	}
	produceRetry, err := toCluster.Producer.Retry.Durations("producer.retry", config.DefaultProducerRetry)
	if err != nil {
		return bridge.RunOptions{}, err
	}
	commitInterval := 0 * time.Millisecond
	if s := strings.TrimSpace(bridgeCfg.CommitInterval); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return bridge.RunOptions{}, fmt.Errorf("bridge.commit_interval: %w", err)
		}
		commitInterval = d
	}
	return bridge.RunOptions{
		PeriodicStatsInterval: periodicStatsInterval,
		BatchSize:             bridgeCfg.EffectiveBatchSize(),
		MaxInFlightBatches:    bridgeCfg.MaxInFlightBatches,
		CommitInterval:        commitInterval,
		CommitMaxRecords:      bridgeCfg.CommitMaxRecords,
		OverridePartition:     bridgeCfg.OverridePartition,
		OverrideKey:           overrideKeyBytes(bridgeCfg.OverrideKey),
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

func overrideKeyBytes(key *string) []byte {
	if key == nil {
		return nil
	}
	return []byte(*key)
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
	if !slog.Default().Enabled(context.Background(), slog.LevelDebug) {
		return
	}
	attrs := baseKafkaClientAttrs(clusterName, clientRole, env)
	switch clientRole {
	case "consumer":
		attrs = append(attrs, consumerKafkaClientAttrs(env)...)
	case "producer":
		attrs = append(attrs, producerKafkaClientAttrs(env)...)
	}

	slog.Debug("kafka client config", attrs...)
}

func baseKafkaClientAttrs(clusterName, clientRole string, env *config.Cluster) []any {
	attrs := []any{
		"cluster", clusterName,
		"client_role", clientRole,
		"seed_broker_count", len(env.Brokers),
		"tls_enabled", env.TLS.Enabled,
	}
	attrs = append(attrs, "sasl_mechanism", env.SASL.EffectiveMechanism())
	if id := strings.TrimSpace(env.Client.ClientID); id != "" {
		attrs = append(attrs, "client_id", id)
	}
	if d := strings.TrimSpace(env.Client.DialTimeout); d != "" {
		attrs = append(attrs, "client_dial_timeout", d)
	}
	if ro := strings.TrimSpace(env.Client.RequestTimeoutOverhead); ro != "" {
		attrs = append(attrs, "client_request_timeout_overhead", ro)
	}
	return attrs
}

func consumerKafkaClientAttrs(env *config.Cluster) []any {
	attrs := []any{"consumer_isolation_level", env.Consumer.EffectiveIsolationLevel()}
	if env.Consumer.FetchMaxBytes != nil {
		attrs = append(attrs, "consumer_fetch_max_bytes", *env.Consumer.FetchMaxBytes)
	}
	if env.Consumer.FetchMaxPartitionBytes != nil {
		attrs = append(attrs, "consumer_fetch_max_partition_bytes", *env.Consumer.FetchMaxPartitionBytes)
	}
	if w := strings.TrimSpace(env.Consumer.FetchMaxWait); w != "" {
		attrs = append(attrs, "consumer_fetch_max_wait", w)
	}
	return attrs
}

func producerKafkaClientAttrs(env *config.Cluster) []any {
	attrs := []any{"producer_required_acks", env.Producer.EffectiveRequiredAcks()}
	if c := strings.TrimSpace(env.Producer.BatchCompression); c != "" {
		attrs = append(attrs, "producer_batch_compression", c)
	}
	if env.Producer.BatchMaxBytes != nil {
		attrs = append(attrs, "producer_batch_max_bytes", *env.Producer.BatchMaxBytes)
	}
	if l := strings.TrimSpace(env.Producer.Linger); l != "" {
		attrs = append(attrs, "producer_linger", l)
	}
	return attrs
}
