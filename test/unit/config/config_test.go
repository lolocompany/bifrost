package config_test

import (
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/internal/config"
)

func TestParse_clustersAndBridges(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
metrics:
  enabled: true
  listen_addr: ":9090"
  groups:
    golang: true
    process: true
    kafka: true
    tls: true
    tcp: true
logging:
  level: info
  format: json
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Clusters["east"].Brokers[0] != "127.0.0.1:9092" {
		t.Fatalf("east broker: %+v", cfg.Clusters["east"])
	}
	if cfg.Bridges[0].EffectiveConsumerGroup() != "bifrost-east-to-west" {
		t.Fatalf("default consumer group: %q", cfg.Bridges[0].EffectiveConsumerGroup())
	}
	if cfg.Clusters["east"].AutoCreateTopics {
		t.Fatalf("default auto_create_topics: want false")
	}
	if cfg.Clusters["west"].AutoCreateTopics {
		t.Fatalf("default auto_create_topics: want false")
	}
}

func TestParse_clusterAutoCreateTopics(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    auto_create_topics: true
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.Clusters["east"].AutoCreateTopics {
		t.Fatalf("auto_create_topics: want true")
	}
}

func TestParse_loggingFormatDefaultJSON(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
metrics:
  enabled: true
  listen_addr: ":9090"
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Logging.Format != "json" {
		t.Fatalf("default log format: want json, got %q", cfg.Logging.Format)
	}
	d, err := cfg.Logging.ParsePeriodicStatsInterval()
	if err != nil {
		t.Fatalf("ParsePeriodicStatsInterval: %v", err)
	}
	if want := 5 * time.Minute; d != want {
		t.Fatalf("default periodic_stats_interval: want %v, got %v", want, d)
	}
}

func TestParse_loggingPeriodicStatsIntervalDisabled(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
  periodic_stats_interval: "0"
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	d, err := cfg.Logging.ParsePeriodicStatsInterval()
	if err != nil {
		t.Fatalf("ParsePeriodicStatsInterval: %v", err)
	}
	if d != 0 {
		t.Fatalf("disabled periodic stats: want 0, got %v", d)
	}
}

func TestParse_metricsListenDefaultsWhenOmitted(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if !cfg.Metrics.MetricsEnabled() {
		t.Fatal("metrics should default to enabled")
	}
	if cfg.Metrics.ListenAddr != ":9090" {
		t.Fatalf("metrics listen_addr: want :9090, got %q", cfg.Metrics.ListenAddr)
	}
}

func TestParse_metricsDisabledWithoutListenAddr(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Metrics.MetricsEnabled() {
		t.Fatal("metrics should be disabled")
	}
	if cfg.Metrics.ListenAddr != "" {
		t.Fatalf("listen_addr should stay empty when metrics disabled, got %q", cfg.Metrics.ListenAddr)
	}
}

func TestParse_loggingFormatAcceptsLogfmt(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  format: logfmt
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("expected logfmt to parse, got: %v", err)
	}
}

func TestParse_loggingExtraFieldsNoDefaults(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if len(cfg.Logging.ExtraFields) != 0 {
		t.Fatalf("expected no default extra_fields, got: %#v", cfg.Logging.ExtraFields)
	}
}

func TestParse_returnsErrorOnInvalid(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges: []
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected parse error from invalid config")
	}
}

func TestEffectiveConsumerGroup_explicit(t *testing.T) {
	b := config.Bridge{Name: "x", ConsumerGroup: "my-group"}
	if b.EffectiveConsumerGroup() != "my-group" {
		t.Fatalf("got %q", b.EffectiveConsumerGroup())
	}
}

func TestBridgeEffectiveBatchSize_defaultAndExplicit(t *testing.T) {
	t.Run("default", func(t *testing.T) {
		b := config.Bridge{}
		if got := b.EffectiveBatchSize(); got != config.DefaultBridgeBatchSize {
			t.Fatalf("default batch size: want %d, got %d", config.DefaultBridgeBatchSize, got)
		}
	})

	t.Run("zero_uses_default_batch_size", func(t *testing.T) {
		b := config.Bridge{BatchSize: 0}
		if got := b.EffectiveBatchSize(); got != config.DefaultBridgeBatchSize {
			t.Fatalf("zero batch size: want %d, got %d", config.DefaultBridgeBatchSize, got)
		}
	})

	t.Run("explicit", func(t *testing.T) {
		b := config.Bridge{BatchSize: 32}
		if got := b.EffectiveBatchSize(); got != 32 {
			t.Fatalf("explicit batch size: want 32, got %d", got)
		}
	})
}

func TestBridgeEffectiveOverridePartition(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		b := config.Bridge{}
		if got, ok := b.EffectiveOverridePartition(); ok || got != 0 {
			t.Fatalf("override partition = (%d, %t), want (0, false)", got, ok)
		}
	})

	t.Run("set", func(t *testing.T) {
		partition := int32(7)
		b := config.Bridge{OverridePartition: &partition}
		if got, ok := b.EffectiveOverridePartition(); !ok || got != 7 {
			t.Fatalf("override partition = (%d, %t), want (7, true)", got, ok)
		}
	})
}

func TestBridgeEffectiveOverrideKey(t *testing.T) {
	t.Run("unset", func(t *testing.T) {
		b := config.Bridge{}
		if got, ok := b.EffectiveOverrideKey(); ok || got != "" {
			t.Fatalf("override key = (%q, %t), want (\"\", false)", got, ok)
		}
	})

	t.Run("set", func(t *testing.T) {
		key := ""
		b := config.Bridge{OverrideKey: &key}
		if got, ok := b.EffectiveOverrideKey(); !ok || got != "" {
			t.Fatalf("override key = (%q, %t), want (\"\", true)", got, ok)
		}
	})
}

func TestParse_bridgeReplicasDefaultAndExplicit(t *testing.T) {
	const minimal = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
`
	t.Run("default", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Bridges[0].Replicas != 0 {
			t.Fatalf("default replicas: want 0 (resolved at runtime), got %d", cfg.Bridges[0].Replicas)
		}
	})
	t.Run("explicit", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    replicas: 4
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Bridges[0].Replicas != 4 {
			t.Fatalf("replicas: want 4, got %d", cfg.Bridges[0].Replicas)
		}
	})
	t.Run("zero_means_topic_partitions", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    replicas: 0
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Bridges[0].Replicas != 0 {
			t.Fatalf("replicas 0: want 0 in config, got %d", cfg.Bridges[0].Replicas)
		}
	})
}

func TestParse_bridgeReplicasNegative(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    replicas: -1
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("Parse: expected error for negative replicas")
	}
}

func TestParse_bridgeReplicasTooLarge(t *testing.T) {
	yamlDoc := fmt.Sprintf(`
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    replicas: %d
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`, config.MaxReplicas+1)
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("Parse: expected error for replicas above max")
	}
}

func TestParse_bridgeBatchSizeDefaultAndExplicit(t *testing.T) {
	const minimal = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
`
	t.Run("default", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if got := cfg.Bridges[0].EffectiveBatchSize(); got != config.DefaultBridgeBatchSize {
			t.Fatalf("default batch size: want %d, got %d", config.DefaultBridgeBatchSize, got)
		}
	})

	t.Run("explicit", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    batch_size: 16
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Bridges[0].BatchSize != 16 {
			t.Fatalf("batch_size: want 16, got %d", cfg.Bridges[0].BatchSize)
		}
		if got := cfg.Bridges[0].EffectiveBatchSize(); got != 16 {
			t.Fatalf("effective batch size: want 16, got %d", got)
		}
	})
}

func TestParse_bridgeBatchSizeNegative(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    batch_size: -1
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("Parse: expected error for negative batch_size")
	}
}

func TestParse_bridgeOverridePartitionDefaultAndExplicit(t *testing.T) {
	const minimal = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
`
	t.Run("default", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if got, ok := cfg.Bridges[0].EffectiveOverridePartition(); ok || got != 0 {
			t.Fatalf("default override partition = (%d, %t), want (0, false)", got, ok)
		}
	})

	t.Run("explicit", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    override_partition: 5
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Bridges[0].OverridePartition == nil {
			t.Fatal("override_partition pointer: want non-nil")
		}
		if got, ok := cfg.Bridges[0].EffectiveOverridePartition(); !ok || got != 5 {
			t.Fatalf("override partition = (%d, %t), want (5, true)", got, ok)
		}
	})
}

func TestParse_bridgeOverridePartitionNegative(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    override_partition: -1
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("Parse: expected error for negative override_partition")
	}
}

func TestParse_bridgeOverrideKeyDefaultAndExplicit(t *testing.T) {
	const minimal = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
`
	t.Run("default", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if got, ok := cfg.Bridges[0].EffectiveOverrideKey(); ok || got != "" {
			t.Fatalf("default override key = (%q, %t), want (\"\", false)", got, ok)
		}
	})

	t.Run("explicit", func(t *testing.T) {
		cfg, err := config.Parse([]byte(minimal + `
bridges:
  - name: east-to-west
    override_key: "static-key"
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`))
		if err != nil {
			t.Fatalf("Parse: %v", err)
		}
		if cfg.Bridges[0].OverrideKey == nil {
			t.Fatal("override_key pointer: want non-nil")
		}
		if got, ok := cfg.Bridges[0].EffectiveOverrideKey(); !ok || got != "static-key" {
			t.Fatalf("override key = (%q, %t), want (\"static-key\", true)", got, ok)
		}
	})
}

func TestParse_clusterKafkaTuning(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    client:
      client_id: bifrost-east
      dial_timeout: "15s"
      broker_max_write_bytes: 16777216
    consumer:
      fetch_max_partition_bytes: 5242880
      session_timeout: "45s"
      isolation_level: read_committed
    producer:
      required_acks: all
      batch_max_bytes: 1048576
      batch_compression: zstd
      linger: "5ms"
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	east := cfg.Clusters["east"]
	if east.Client.ClientID != "bifrost-east" {
		t.Fatalf("client_id: %q", east.Client.ClientID)
	}
	if east.Consumer.IsolationLevel != "read_committed" {
		t.Fatalf("isolation: %q", east.Consumer.IsolationLevel)
	}
	if east.Producer.BatchCompression != "zstd" {
		t.Fatalf("compression: %q", east.Producer.BatchCompression)
	}
}

func TestParse_clusterCrossFieldBrokerReadVsFetch(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    client:
      broker_max_read_bytes: 33554432
    consumer:
      fetch_max_bytes: 52428800
bridges:
  - name: east-to-west
    from: { cluster: east, topic: a }
    to: { cluster: east, topic: b }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error when broker_max_read_bytes < fetch_max_bytes")
	}
}

func TestParse_clusterCrossFieldBrokerWriteVsBatch(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    client:
      broker_max_write_bytes: 1048576
    producer:
      batch_max_bytes: 2097152
bridges:
  - name: east-to-west
    from: { cluster: east, topic: a }
    to: { cluster: east, topic: b }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error when broker_max_write_bytes < batch_max_bytes")
	}
}

func TestParse_clusterCrossFieldFetchVsPartition(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      fetch_max_bytes: 1048576
      fetch_max_partition_bytes: 2097152
bridges:
  - name: east-to-west
    from: { cluster: east, topic: a }
    to: { cluster: east, topic: b }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error when fetch_max_bytes < fetch_max_partition_bytes")
	}
}

func TestParse_clusterCrossFieldHeartbeatVsSession(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      session_timeout: "10s"
      heartbeat_interval: "10s"
bridges:
  - name: east-to-west
    from: { cluster: east, topic: a }
    to: { cluster: east, topic: b }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error when heartbeat_interval >= session_timeout")
	}
}

func TestParse_clusterCrossFieldRebalanceVsSession(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      session_timeout: "60s"
      rebalance_timeout: "30s"
bridges:
  - name: east-to-west
    from: { cluster: east, topic: a }
    to: { cluster: east, topic: b }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error when rebalance_timeout < session_timeout")
	}
}

func TestParse_clusterInvalidConsumerIsolation(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      isolation_level: transactional
bridges:
  - name: east-to-west
    from: { cluster: east, topic: a }
    to: { cluster: east, topic: b }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for invalid isolation_level")
	}
}

func TestParse_metricsExtraLabels(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: true
  listen_addr: ":9090"
  extra_labels:
    service: bifrost
    env: prod
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Metrics.ExtraLabels["service"] != "bifrost" {
		t.Fatalf("extra_labels.service: %q", cfg.Metrics.ExtraLabels["service"])
	}
}

func TestParse_metricsExtraLabelsInvalid(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: true
  listen_addr: ":9090"
  extra_labels:
    "bridge": source
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for conflicting metrics.extra_labels key")
	}
}

func TestExampleConfigParses(t *testing.T) {
	data, err := os.ReadFile("../../../example.config.yaml")
	if err != nil {
		t.Fatalf("ReadFile example.config.yaml: %v", err)
	}
	if _, err := config.Parse(data); err != nil {
		t.Fatalf("Parse example.config.yaml: %v", err)
	}
}

func TestParse_rejectsSASLMechanismWithoutPassword(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    sasl:
      mechanism: plain
      username: alice
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for SASL mechanism without password")
	}
}

func TestParse_rejectsUnknownTopLevelField(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
unexpected_top_level: true
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for unknown top-level field")
	}
}

func TestParse_rejectsUnknownNestedField(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
  bogus_field: true
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for unknown nested logging field")
	}
}

func TestParse_strictDecodingStillAllowsArbitraryMapKeys(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: true
  listen_addr: ":9090"
  extra_labels:
    service: bifrost
    env: test
logging:
  level: info
  stream: stdout
  extra_fields:
    schema_version: "1.0"
    service: bifrost
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Metrics.ExtraLabels["service"] != "bifrost" {
		t.Fatalf("metrics.extra_labels.service: %q", cfg.Metrics.ExtraLabels["service"])
	}
	if cfg.Logging.ExtraFields["schema_version"] != "1.0" {
		t.Fatalf("logging.extra_fields.schema_version: %q", cfg.Logging.ExtraFields["schema_version"])
	}
}

func TestParse_rejectsInvalidBrokerAddress(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["localhost"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for broker without host:port")
	}
}

func TestParse_rejectsInvalidTLSCAFile(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    tls:
      enabled: true
      ca_file: "/definitely/missing/ca.pem"
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for invalid tls.ca_file")
	}
}

func TestParse_rejectsFileLoggingStream(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: file
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for unsupported file logging stream")
	}
}

func TestParse_normalizesWhitespaceAcrossConfig(t *testing.T) {
	const yamlDoc = `
clusters:
  " east ":
    brokers: [" 127.0.0.1:9092 "]
bridges:
  - name: " east-to-east "
    from: { cluster: " east ", topic: " in " }
    to: { cluster: " east ", topic: " out " }
    consumer_group: " cg-east "
metrics:
  enabled: false
  extra_labels:
    " service ": " bifrost "
logging:
  level: " info "
  format: " json "
  stream: " stdout "
  extra_fields:
    " schema_version ": " 1.0 "
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	east, ok := cfg.Clusters["east"]
	if !ok {
		t.Fatalf("expected normalized cluster key %q in %#v", "east", cfg.Clusters)
	}
	if east.Brokers[0] != "127.0.0.1:9092" {
		t.Fatalf("normalized broker: got %q", east.Brokers[0])
	}
	if cfg.Bridges[0].Name != "east-to-east" {
		t.Fatalf("normalized bridge name: got %q", cfg.Bridges[0].Name)
	}
	if cfg.Bridges[0].From.Topic != "in" || cfg.Bridges[0].To.Topic != "out" {
		t.Fatalf("normalized topics: from=%q to=%q", cfg.Bridges[0].From.Topic, cfg.Bridges[0].To.Topic)
	}
	if cfg.Bridges[0].ConsumerGroup != "cg-east" {
		t.Fatalf("normalized consumer group: got %q", cfg.Bridges[0].ConsumerGroup)
	}
	if cfg.Logging.Stream != "stdout" || cfg.Logging.Level != "info" || cfg.Logging.Format != "json" {
		t.Fatalf("normalized logging config: %#v", cfg.Logging)
	}
	if cfg.Logging.ExtraFields["schema_version"] != "1.0" {
		t.Fatalf("normalized extra field: %#v", cfg.Logging.ExtraFields)
	}
	if cfg.Metrics.ExtraLabels["service"] != "bifrost" {
		t.Fatalf("normalized extra label: %#v", cfg.Metrics.ExtraLabels)
	}
}

func TestParse_retryConfigParses(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      commit_retry:
        min_backoff: "250ms"
        max_backoff: "5s"
        jitter: "50ms"
    producer:
      retry:
        min_backoff: "500ms"
        max_backoff: "10s"
        jitter: "100ms"
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Clusters["east"].Consumer.CommitRetry.MinBackoff != "250ms" {
		t.Fatalf("consumer.commit_retry.min_backoff: got %q", cfg.Clusters["east"].Consumer.CommitRetry.MinBackoff)
	}
	if cfg.Clusters["east"].Producer.Retry.MaxBackoff != "10s" {
		t.Fatalf("producer.retry.max_backoff: got %q", cfg.Clusters["east"].Producer.Retry.MaxBackoff)
	}
}

func TestParse_retryConfigRejectsInvalidDuration(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    producer:
      retry:
        min_backoff: "fast"
        max_backoff: "10s"
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for invalid retry duration")
	}
}

func TestParse_retryConfigRejectsMinGreaterThanMax(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      commit_retry:
        min_backoff: "10s"
        max_backoff: "1s"
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error when retry min_backoff > max_backoff")
	}
}

func TestParse_retryConfigNormalizesWhitespace(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
    consumer:
      commit_retry:
        min_backoff: " 250ms "
        max_backoff: " 5s "
        jitter: " 50ms "
    producer:
      retry:
        min_backoff: " 500ms "
        max_backoff: " 10s "
        jitter: " 100ms "
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Clusters["east"].Consumer.CommitRetry.MinBackoff != "250ms" {
		t.Fatalf("normalized consumer min_backoff: got %q", cfg.Clusters["east"].Consumer.CommitRetry.MinBackoff)
	}
	if cfg.Clusters["east"].Producer.Retry.Jitter != "100ms" {
		t.Fatalf("normalized producer jitter: got %q", cfg.Clusters["east"].Producer.Retry.Jitter)
	}
}

func TestParse_bridgeExtraHeaders(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
    extra_headers:
      env: prod
      trace: "x"
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Bridges[0].ExtraHeaders["env"] != "prod" || cfg.Bridges[0].ExtraHeaders["trace"] != "x" {
		t.Fatalf("extra_headers: %+v", cfg.Bridges[0].ExtraHeaders)
	}
}

func TestParse_bridgeExtraHeadersRejectsReservedKey(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
    extra_headers:
      bifrost.source.cluster: "nope"
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for reserved extra_headers key")
	}
}

func TestParse_bridgeExtraHeadersRejectsBifrostPrefix(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
    extra_headers:
      bifrost.custom: "nope"
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for bifrost.* extra_headers key")
	}
}

func TestParse_bridgeExtraHeadersRejectsEmptyKey(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from:
      cluster: east
      topic: incoming
    to:
      cluster: west
      topic: outgoing
    extra_headers:
      "": "v"
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for empty extra_headers key")
	}
}

func TestParse_bridgeThroughputKnobs(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
  west:
    brokers: ["127.0.0.1:9093"]
bridges:
  - name: east-to-west
    from: { cluster: east, topic: in }
    to: { cluster: west, topic: out }
    max_in_flight_batches: 16
    commit_interval: "300ms"
    commit_max_records: 2000
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if got := cfg.Bridges[0].MaxInFlightBatches; got != 16 {
		t.Fatalf("max_in_flight_batches: got %d", got)
	}
	if got := cfg.Bridges[0].CommitInterval; got != "300ms" {
		t.Fatalf("commit_interval: got %q", got)
	}
	if got := cfg.Bridges[0].CommitMaxRecords; got != 2000 {
		t.Fatalf("commit_max_records: got %d", got)
	}
}

func TestParse_bridgeThroughputKnobsRejectsInvalid(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
    max_in_flight_batches: 300
metrics:
  enabled: false
logging:
  level: info
  stream: stdout
`
	if _, err := config.Parse([]byte(yamlDoc)); err == nil {
		t.Fatal("expected error for max_in_flight_batches out of range")
	}
}

func TestParse_appliesCentralizedDefaults(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: true
logging: {}
`
	cfg, err := config.Parse([]byte(yamlDoc))
	if err != nil {
		t.Fatalf("Parse: %v", err)
	}
	if cfg.Logging.Level != "info" || cfg.Logging.Format != "json" || cfg.Logging.Stream != "stdout" {
		t.Fatalf("logging defaults not applied: %#v", cfg.Logging)
	}
	if cfg.Logging.PeriodicStatsInterval != "5m" {
		t.Fatalf("periodic_stats_interval default: got %q", cfg.Logging.PeriodicStatsInterval)
	}
	if cfg.Metrics.ListenAddr != ":9090" {
		t.Fatalf("metrics listen default: got %q", cfg.Metrics.ListenAddr)
	}
	br := cfg.Bridges[0]
	if br.BatchSize != config.DefaultBridgeBatchSize {
		t.Fatalf("bridge batch_size default: got %d", br.BatchSize)
	}
	if br.MaxInFlightBatches != config.DefaultMaxInFlightBatches {
		t.Fatalf("bridge max_in_flight_batches default: got %d", br.MaxInFlightBatches)
	}
	if br.CommitInterval != config.DefaultCommitInterval.String() {
		t.Fatalf("bridge commit_interval default: got %q", br.CommitInterval)
	}
	if br.CommitMaxRecords != config.DefaultCommitMaxRecords {
		t.Fatalf("bridge commit_max_records default: got %d", br.CommitMaxRecords)
	}
	if cfg.Clusters["east"].Client.DialTimeout != config.DefaultClientDialTimeout.String() {
		t.Fatalf("client dial_timeout default: got %q", cfg.Clusters["east"].Client.DialTimeout)
	}
}
