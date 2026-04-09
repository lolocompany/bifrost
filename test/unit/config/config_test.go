package config_test

import (
	"testing"

	"github.com/lolocompany/bifrost/pkg/config"
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
    forward: true
    errors: true
    latency: false
    golang: true
    process: true
    kafka: true
    tls: true
    tcp: true
logging:
  level: info
  format: logfmt
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
	if cfg.Metrics.Groups.GroupLatency() {
		t.Fatal("latency group should be off")
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

func TestMustParse_panicsOnInvalid(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges: []
`
	defer func() {
		if recover() == nil {
			t.Fatal("expected panic from MustParse on invalid config")
		}
	}()
	config.MustParse([]byte(yamlDoc))
}

func TestEffectiveConsumerGroup_explicit(t *testing.T) {
	b := config.Bridge{Name: "x", ConsumerGroup: "my-group"}
	if b.EffectiveConsumerGroup() != "my-group" {
		t.Fatalf("got %q", b.EffectiveConsumerGroup())
	}
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
