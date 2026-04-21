package config

import (
	"strings"
	"time"
)

const (
	DefaultClientDialTimeout    = 10 * time.Second
	DefaultPingTimeout          = 30 * time.Second
	DefaultBridgeBatchSize      = 1
	DefaultMaxInFlightBatches   = 64
	DefaultCommitInterval       = 1 * time.Second
	DefaultCommitMaxRecords     = 1024
	DefaultProducerRequiredAcks = "leader"
	DefaultDisableIdempotentWrite = true
)

// Cluster describes a Kafka cluster connection: seed brokers, security, and optional
// franz-go tuning for shared clients, consumers, and producers.
//
// Large records: If you see MESSAGE_TOO_LARGE (or similar) on produce or fetch, raise
// producer.batch_max_bytes and consumer.fetch_max_partition_bytes (and usually fetch_max_bytes)
// together with client.broker_max_write_bytes / broker_max_read_bytes when needed. You must also
// configure the Kafka/Redpanda brokers to allow that record size (for example message.max.bytes,
// kafka_batch_max_bytes on Redpanda)—bifrost only configures its clients.
type Cluster struct {
	Brokers  []string         `yaml:"brokers"`
	TLS      TLS              `yaml:"tls"`
	SASL     SASL             `yaml:"sasl"`
	Client   ClientSettings   `yaml:"client"`
	Consumer ConsumerSettings `yaml:"consumer"`
	Producer ProducerSettings `yaml:"producer"`

	// AutoCreateTopics, when true, creates each bridge source (from) and destination (to) topic on
	// this cluster at startup if it does not exist (Kafka CreateTopics). Default false: topics must
	// exist or be created by the broker (e.g. broker auto-create) or an operator.
	AutoCreateTopics bool `yaml:"auto_create_topics"`
}

// ClientSettings configures connection-level options shared by every Kafka client to this cluster
// (dialing, request sizing, client id). Omitted fields leave franz-go defaults.
type ClientSettings struct {
	ClientID string `yaml:"client_id"`

	// DialTimeout is how long to wait for a TCP connection (e.g. "10s", "30s").
	DialTimeout string `yaml:"dial_timeout"`
	// RequestTimeoutOverhead is added to per-request deadlines (e.g. "10s").
	RequestTimeoutOverhead string `yaml:"request_timeout_overhead"`

	// BrokerMaxWriteBytes caps a single write to a broker (Kafka socket.request.max.bytes). When
	// producer.batch_max_bytes is set, broker_max_write_bytes must be >= that value.
	BrokerMaxWriteBytes *int32 `yaml:"broker_max_write_bytes"`
	// BrokerMaxReadBytes caps read size from a broker; must be >= consumer.fetch_max_bytes (franz-go validation).
	BrokerMaxReadBytes *int32 `yaml:"broker_max_read_bytes"`
}

// ConsumerSettings configures the consumer-group client used on the from side of a bridge.
type ConsumerSettings struct {
	// FetchMaxBytes and FetchMaxPartitionBytes bound fetch responses; set at least the max record
	// size you expect (otherwise fetches can fail with errors such as MESSAGE_TOO_LARGE).
	// When both are set, fetch_max_bytes must be >= fetch_max_partition_bytes.
	FetchMaxBytes          *int32 `yaml:"fetch_max_bytes"`
	FetchMaxPartitionBytes *int32 `yaml:"fetch_max_partition_bytes"`
	FetchMaxWait           string `yaml:"fetch_max_wait"`
	SessionTimeout         string `yaml:"session_timeout"`
	// HeartbeatInterval must be < session_timeout when both are set; rebalance_timeout must be >= session_timeout when both are set.
	HeartbeatInterval string        `yaml:"heartbeat_interval"`
	RebalanceTimeout  string        `yaml:"rebalance_timeout"`
	IsolationLevel    string        `yaml:"isolation_level"` // read_uncommitted (default), read_committed
	CommitRetry       RetrySettings `yaml:"commit_retry"`
}

// ProducerSettings configures the producer client used on the to side of a bridge.
type ProducerSettings struct {
	// RequiredAcks: leader (default), all, none.
	RequiredAcks string `yaml:"required_acks"`

	// BatchMaxBytes is the max record batch size before compression (Kafka max.message.bytes);
	// set at least your largest record size (or produce can fail with MESSAGE_TOO_LARGE).
	BatchMaxBytes *int32 `yaml:"batch_max_bytes"`

	// BatchCompression: snappy, zstd, lz4, gzip, none; empty uses franz-go defaults.
	BatchCompression string `yaml:"batch_compression"`

	Linger                 string        `yaml:"linger"`
	ProduceRequestTimeout  string        `yaml:"produce_request_timeout"`
	DisableIdempotentWrite *bool         `yaml:"disable_idempotent_write"`
	Retry                  RetrySettings `yaml:"retry"`
}

// RetrySettings configures unbounded retry with exponential backoff and additive jitter.
type RetrySettings struct {
	MinBackoff string `yaml:"min_backoff"`
	MaxBackoff string `yaml:"max_backoff"`
	Jitter     string `yaml:"jitter"`
}

// RetryDurations holds parsed retry durations ready for runtime use.
type RetryDurations struct {
	MinBackoff time.Duration
	MaxBackoff time.Duration
	Jitter     time.Duration
}

var (
	DefaultProducerRetry = RetryDurations{
		MinBackoff: time.Second,
		MaxBackoff: 30 * time.Second,
		Jitter:     250 * time.Millisecond,
	}
	DefaultCommitRetry = RetryDurations{
		MinBackoff: time.Second,
		MaxBackoff: 30 * time.Second,
		Jitter:     250 * time.Millisecond,
	}
)

// TLS configures TLS for broker connections.
type TLS struct {
	Enabled  bool   `yaml:"enabled"`
	CAFile   string `yaml:"ca_file"`
	CertFile string `yaml:"cert_file"`
	KeyFile  string `yaml:"key_file"`
}

// SASL configures SASL authentication.
type SASL struct {
	Mechanism string `yaml:"mechanism"` // none, plain, scram-sha-256, scram-sha-512
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

func (c *Cluster) ApplyDefaults() {
	c.Client.ApplyDefaults()
	c.Producer.ApplyDefaults()
}

func (c *ClientSettings) ApplyDefaults() {
	if c == nil {
		return
	}
	if c.DialTimeout == "" {
		c.DialTimeout = DefaultClientDialTimeout.String()
	}
}

func (c *ClientSettings) DialTimeoutDuration() (time.Duration, error) {
	if c == nil {
		return DefaultClientDialTimeout, nil
	}
	if c.DialTimeout == "" {
		return DefaultClientDialTimeout, nil
	}
	return time.ParseDuration(c.DialTimeout)
}

func (c *ClientSettings) PingTimeoutDuration() (time.Duration, error) {
	if c == nil || c.DialTimeout == "" {
		return DefaultPingTimeout, nil
	}
	return time.ParseDuration(c.DialTimeout)
}

func (s SASL) EffectiveMechanism() string {
	mech := strings.ToLower(strings.TrimSpace(s.Mechanism))
	if mech == "" {
		return "none"
	}
	return mech
}

func (c ConsumerSettings) EffectiveIsolationLevel() string {
	level := strings.ToLower(strings.TrimSpace(c.IsolationLevel))
	if level == "" {
		return "read_uncommitted"
	}
	return level
}

func (p ProducerSettings) EffectiveRequiredAcks() string {
	acks := strings.ToLower(strings.TrimSpace(p.RequiredAcks))
	if acks == "" {
		return DefaultProducerRequiredAcks
	}
	return acks
}

func (p *ProducerSettings) ApplyDefaults() {
	if p == nil {
		return
	}
	if strings.TrimSpace(p.RequiredAcks) == "" {
		p.RequiredAcks = DefaultProducerRequiredAcks
	}
	if p.DisableIdempotentWrite == nil {
		v := DefaultDisableIdempotentWrite
		p.DisableIdempotentWrite = &v
	}
}
