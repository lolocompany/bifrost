package config

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"
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
	// RequiredAcks: all (default), leader, none.
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

func (c *Cluster) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers: at least one broker is required")
	}
	for i, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return fmt.Errorf("brokers[%d]: must not be empty", i)
		}
		host, port, err := net.SplitHostPort(b)
		if err != nil {
			return fmt.Errorf("brokers[%d]: must be host:port: %w", i, err)
		}
		if strings.TrimSpace(host) == "" {
			return fmt.Errorf("brokers[%d]: host must not be empty", i)
		}
		if _, err := net.LookupPort("tcp", port); err != nil {
			return fmt.Errorf("brokers[%d]: invalid port %q: %w", i, port, err)
		}
	}
	if err := c.TLS.validate(); err != nil {
		return fmt.Errorf("tls: %w", err)
	}
	if err := c.SASL.validate(); err != nil {
		return fmt.Errorf("sasl: %w", err)
	}
	if err := c.Client.validate(); err != nil {
		return fmt.Errorf("client: %w", err)
	}
	if err := c.Consumer.validate(); err != nil {
		return fmt.Errorf("consumer: %w", err)
	}
	if err := c.Producer.validate(); err != nil {
		return fmt.Errorf("producer: %w", err)
	}
	if err := c.validateCrossField(); err != nil {
		return err
	}
	return nil
}

// validateCrossField enforces relationships between client, consumer, and producer settings
// (mirrors franz-go client validation so misconfigurations fail at load time).
func (c *Cluster) validateCrossField() error {
	cl := &c.Client
	co := &c.Consumer
	pr := &c.Producer

	if cl.BrokerMaxWriteBytes != nil && pr.BatchMaxBytes != nil {
		if *cl.BrokerMaxWriteBytes < *pr.BatchMaxBytes {
			return fmt.Errorf("client.broker_max_write_bytes (%d) must be >= producer.batch_max_bytes (%d)", *cl.BrokerMaxWriteBytes, *pr.BatchMaxBytes)
		}
	}
	if cl.BrokerMaxReadBytes != nil && co.FetchMaxBytes != nil {
		if *cl.BrokerMaxReadBytes < *co.FetchMaxBytes {
			return fmt.Errorf("client.broker_max_read_bytes (%d) must be >= consumer.fetch_max_bytes (%d)", *cl.BrokerMaxReadBytes, *co.FetchMaxBytes)
		}
	}
	if co.FetchMaxBytes != nil && co.FetchMaxPartitionBytes != nil {
		if *co.FetchMaxBytes < *co.FetchMaxPartitionBytes {
			return fmt.Errorf("consumer.fetch_max_bytes (%d) must be >= consumer.fetch_max_partition_bytes (%d)", *co.FetchMaxBytes, *co.FetchMaxPartitionBytes)
		}
	}

	hb, hbSet, err := parseDurationIfSet("consumer.heartbeat_interval", co.HeartbeatInterval)
	if err != nil {
		return err
	}
	sess, sessSet, err := parseDurationIfSet("consumer.session_timeout", co.SessionTimeout)
	if err != nil {
		return err
	}
	if hbSet && sessSet && hb >= sess {
		return fmt.Errorf("consumer.heartbeat_interval (%v) must be less than consumer.session_timeout (%v)", hb, sess)
	}

	reb, rebSet, err := parseDurationIfSet("consumer.rebalance_timeout", co.RebalanceTimeout)
	if err != nil {
		return err
	}
	if rebSet && sessSet && reb < sess {
		return fmt.Errorf("consumer.rebalance_timeout (%v) must be >= consumer.session_timeout (%v)", reb, sess)
	}

	return nil
}

func parseDurationIfSet(field, s string) (d time.Duration, set bool, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false, nil
	}
	d, err = time.ParseDuration(s)
	if err != nil {
		return 0, false, fmt.Errorf("%s: parse duration: %w", field, err)
	}
	return d, true, nil
}

func parseOptionalDuration(field, s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("%s: parse duration: %w", field, err)
	}
	return d, nil
}

func (c *ClientSettings) validate() error {
	if _, err := parseOptionalDuration("dial_timeout", c.DialTimeout); err != nil {
		return err
	}
	if _, err := parseOptionalDuration("request_timeout_overhead", c.RequestTimeoutOverhead); err != nil {
		return err
	}
	if c.BrokerMaxWriteBytes != nil && *c.BrokerMaxWriteBytes <= 0 {
		return errors.New("broker_max_write_bytes must be positive when set")
	}
	if c.BrokerMaxReadBytes != nil && *c.BrokerMaxReadBytes <= 0 {
		return errors.New("broker_max_read_bytes must be positive when set")
	}
	return nil
}

func (c *ConsumerSettings) validate() error {
	if c.FetchMaxBytes != nil && *c.FetchMaxBytes <= 0 {
		return errors.New("fetch_max_bytes must be positive when set")
	}
	if c.FetchMaxPartitionBytes != nil && *c.FetchMaxPartitionBytes <= 0 {
		return errors.New("fetch_max_partition_bytes must be positive when set")
	}
	for _, pair := range []struct {
		field string
		value string
	}{
		{"fetch_max_wait", c.FetchMaxWait},
		{"session_timeout", c.SessionTimeout},
		{"heartbeat_interval", c.HeartbeatInterval},
		{"rebalance_timeout", c.RebalanceTimeout},
	} {
		if _, err := parseOptionalDuration(pair.field, pair.value); err != nil {
			return err
		}
	}
	switch strings.ToLower(strings.TrimSpace(c.IsolationLevel)) {
	case "", "read_uncommitted", "read_committed":
	default:
		return fmt.Errorf("isolation_level: unsupported %q (use read_uncommitted, read_committed)", c.IsolationLevel)
	}
	if _, err := c.CommitRetry.Durations("commit_retry", DefaultCommitRetry); err != nil {
		return err
	}
	return nil
}

func (p *ProducerSettings) validate() error {
	if err := validRequiredAcks(p.RequiredAcks); err != nil {
		return err
	}
	if p.BatchMaxBytes != nil && *p.BatchMaxBytes <= 0 {
		return errors.New("batch_max_bytes must be positive when set")
	}
	if err := validateCompressionName(p.BatchCompression); err != nil {
		return err
	}
	for _, pair := range []struct {
		field string
		value string
	}{
		{"linger", p.Linger},
		{"produce_request_timeout", p.ProduceRequestTimeout},
	} {
		if _, err := parseOptionalDuration(pair.field, pair.value); err != nil {
			return err
		}
	}
	if _, err := p.Retry.Durations("retry", DefaultProducerRetry); err != nil {
		return err
	}
	return nil
}

func (r RetrySettings) Durations(field string, defaults RetryDurations) (RetryDurations, error) {
	out := defaults

	if s := strings.TrimSpace(r.MinBackoff); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return RetryDurations{}, fmt.Errorf("%s.min_backoff: parse duration: %w", field, err)
		}
		if d <= 0 {
			return RetryDurations{}, fmt.Errorf("%s.min_backoff: must be positive", field)
		}
		out.MinBackoff = d
	}

	if s := strings.TrimSpace(r.MaxBackoff); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return RetryDurations{}, fmt.Errorf("%s.max_backoff: parse duration: %w", field, err)
		}
		if d <= 0 {
			return RetryDurations{}, fmt.Errorf("%s.max_backoff: must be positive", field)
		}
		out.MaxBackoff = d
	}

	if s := strings.TrimSpace(r.Jitter); s != "" {
		d, err := time.ParseDuration(s)
		if err != nil {
			return RetryDurations{}, fmt.Errorf("%s.jitter: parse duration: %w", field, err)
		}
		if d < 0 {
			return RetryDurations{}, fmt.Errorf("%s.jitter: must not be negative", field)
		}
		out.Jitter = d
	}

	if out.MaxBackoff < out.MinBackoff {
		return RetryDurations{}, fmt.Errorf("%s.max_backoff (%v) must be >= %s.min_backoff (%v)", field, out.MaxBackoff, field, out.MinBackoff)
	}

	return out, nil
}

func validRequiredAcks(s string) error {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all", "-1", "none", "0", "leader", "1":
		return nil
	default:
		return fmt.Errorf("required_acks: unsupported %q (use all, leader, none)", s)
	}
}

func validateCompressionName(s string) error {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "snappy", "zstd", "lz4", "gzip", "none":
		return nil
	default:
		return fmt.Errorf("batch_compression: unsupported %q (use snappy, zstd, lz4, gzip, none)", s)
	}
}

func (t *TLS) validate() error {
	if t == nil || !t.Enabled {
		return nil
	}
	if t.CAFile != "" {
		ca, err := os.ReadFile(t.CAFile)
		if err != nil {
			return fmt.Errorf("ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return errors.New("ca_file: no valid PEM certificates")
		}
	}
	if t.CertFile != "" || t.KeyFile != "" {
		if t.CertFile == "" || t.KeyFile == "" {
			return errors.New("cert_file and key_file must both be set for mutual TLS")
		}
		if _, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile); err != nil {
			return fmt.Errorf("client certificate: %w", err)
		}
	}
	return nil
}

func (s *SASL) validate() error {
	if s == nil {
		return nil
	}
	mech := strings.ToLower(strings.TrimSpace(s.Mechanism))
	if mech == "" || mech == "none" {
		return nil
	}
	switch mech {
	case "plain", "scram-sha-256", "scram-sha-512":
		if strings.TrimSpace(s.Username) == "" {
			return fmt.Errorf("username is required for mechanism %q", mech)
		}
		if strings.TrimSpace(s.Password) == "" {
			return fmt.Errorf("password is required for mechanism %q", mech)
		}
	default:
		return fmt.Errorf("unsupported mechanism %q (use none, plain, scram-sha-256, scram-sha-512)", s.Mechanism)
	}
	return nil
}
