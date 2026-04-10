// Package config loads and validates bridge YAML configuration.
//
// Parse and Load return an error when required fields are missing or invalid. applyDefaults fills
// sensible defaults (logging level/format/stream, metrics listen address when metrics are enabled).
// MustParse and MustLoad panic instead of returning an error—use only when startup must abort on bad config.
package config

import (
	"errors"
	"fmt"
	"net"
	"os"
	"regexp"
	"strings"
	"time"
	"unicode"

	"gopkg.in/yaml.v3"
)

// Config is the top-level configuration for bifrost.
type Config struct {
	Clusters map[string]Cluster `yaml:"clusters"`
	Bridges  []Bridge           `yaml:"bridges"`
	Metrics  Metrics            `yaml:"metrics"`
	Logging  Logging            `yaml:"logging"`
}

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
	HeartbeatInterval string `yaml:"heartbeat_interval"`
	RebalanceTimeout  string `yaml:"rebalance_timeout"`
	IsolationLevel    string `yaml:"isolation_level"` // read_uncommitted (default), read_committed
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

	Linger                 string `yaml:"linger"`
	ProduceRequestTimeout  string `yaml:"produce_request_timeout"`
	DisableIdempotentWrite *bool  `yaml:"disable_idempotent_write"`
}

// TLS configures TLS for broker connections.
type TLS struct {
	Enabled            bool   `yaml:"enabled"`
	CAFile             string `yaml:"ca_file"`
	CertFile           string `yaml:"cert_file"`
	KeyFile            string `yaml:"key_file"`
	InsecureSkipVerify bool   `yaml:"insecure_skip_verify"`
}

// SASL configures SASL authentication.
type SASL struct {
	Mechanism string `yaml:"mechanism"` // none, plain, scram-sha-256, scram-sha-512
	Username  string `yaml:"username"`
	Password  string `yaml:"password"`
}

// Bridge defines one directional relay between two clusters and topics (from → to).
type Bridge struct {
	Name          string       `yaml:"name"`
	From          BridgeTarget `yaml:"from"`
	To            BridgeTarget `yaml:"to"`
	ConsumerGroup string       `yaml:"consumer_group"`
}

// BridgeTarget references a cluster name (key in clusters) and a single topic.
type BridgeTarget struct {
	Cluster string `yaml:"cluster"`
	Topic   string `yaml:"topic"`
}

// Metrics configures the Prometheus scrape endpoint and which series to expose.
type Metrics struct {
	Enable     *bool        `yaml:"enabled"`     // default true when omitted
	ListenAddr string       `yaml:"listen_addr"` // default :9090 when metrics are enabled and omitted/blank
	Groups     MetricGroups `yaml:"groups"`
	ExtraLabels map[string]string `yaml:"extra_labels"` // added to all bifrost metrics as constant labels
}

// MetricGroups toggles metric families. Omitted or nil fields default to enabled.
// When enabled, application metric families use the bifrost_ namespace (bifrost_relay_, bifrost_kafka_,
// bifrost_tls_, bifrost_tcp_). The forward/errors/latency group switches together control the relay metric
// families. The golang/process groups expose standard
// client_golang collector names (go_* and process_*). See pkg/metrics for details.
type MetricGroups struct {
	Forward *bool `yaml:"forward"`
	Errors  *bool `yaml:"errors"`
	Latency *bool `yaml:"latency"`
	Golang  *bool `yaml:"golang"`  // Go runtime + build info collectors
	Process *bool `yaml:"process"` // process (CPU/mem/fd) collector
	Kafka   *bool `yaml:"kafka"`   // franz-go broker wire / throttle metrics
	TLS     *bool `yaml:"tls"`     // TLS handshake + peer cert from broker connections
	TCP     *bool `yaml:"tcp"`     // TCP broker socket metrics from franz-go hooks (cross-platform)
}

// Logging configures process logging (slog).
type Logging struct {
	Level       string            `yaml:"level"`        // trace, debug, info, warn, error, fatal
	Format      string            `yaml:"format"`       // json (default) or logfmt
	Stream      string            `yaml:"stream"`       // stdout, stderr, file
	FilePath    string            `yaml:"file_path"`    // required when stream is file
	ExtraFields map[string]string `yaml:"extra_fields"` // always included on every log line
}

// Load reads and parses a YAML file from path.
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read config file: %w", err)
	}
	return Parse(data)
}

// MustLoad calls Load and panics if the file cannot be read or the config is invalid.
// Prefer Load for library use; use MustLoad only when startup must abort the process on bad config.
func MustLoad(path string) *Config {
	cfg, err := Load(path)
	if err != nil {
		panic(fmt.Sprintf("bifrost: invalid config: %v", err))
	}
	return cfg
}

// Parse unmarshals YAML bytes into Config and validates it.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	if err := yaml.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// MustParse calls Parse and panics if the YAML is invalid.
func MustParse(data []byte) *Config {
	cfg, err := Parse(data)
	if err != nil {
		panic(fmt.Sprintf("bifrost: invalid config: %v", err))
	}
	return cfg
}

func (c *Config) applyDefaults() {
	if strings.TrimSpace(c.Logging.Level) == "" {
		c.Logging.Level = "info"
	}
	if strings.TrimSpace(c.Logging.Format) == "" {
		c.Logging.Format = "json"
	}
	if strings.TrimSpace(c.Logging.Stream) == "" {
		c.Logging.Stream = "stdout"
	}
	// When metrics are on, listen_addr defaults so a minimal config can omit the whole metrics section.
	if c.Metrics.MetricsEnabled() {
		if addr := strings.TrimSpace(c.Metrics.ListenAddr); addr == "" {
			c.Metrics.ListenAddr = ":9090"
		} else {
			c.Metrics.ListenAddr = addr
		}
	}
}

// Validate checks required fields and invariants.
func (c *Config) Validate() error {
	if c == nil {
		return errors.New("config is nil")
	}
	if len(c.Clusters) == 0 {
		return errors.New("clusters: at least one cluster is required")
	}
	for name, cl := range c.Clusters {
		if strings.TrimSpace(name) == "" {
			return errors.New("clusters: cluster name must not be empty")
		}
		if err := cl.validate(); err != nil {
			return fmt.Errorf("clusters[%q]: %w", name, err)
		}
	}
	if len(c.Bridges) == 0 {
		return errors.New("bridges: at least one bridge is required")
	}
	seen := make(map[string]struct{})
	for i, b := range c.Bridges {
		if err := b.validate(c.Clusters); err != nil {
			return fmt.Errorf("bridges[%d]: %w", i, err)
		}
		key := strings.TrimSpace(b.Name)
		if _, dup := seen[key]; dup {
			return fmt.Errorf("bridges: duplicate name %q", key)
		}
		seen[key] = struct{}{}
	}
	if err := c.Metrics.validate(); err != nil {
		return fmt.Errorf("metrics: %w", err)
	}
	if err := c.Logging.validate(); err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	return nil
}

func (c *Cluster) validate() error {
	if len(c.Brokers) == 0 {
		return errors.New("brokers: at least one broker is required")
	}
	for i, b := range c.Brokers {
		if strings.TrimSpace(b) == "" {
			return fmt.Errorf("brokers[%d]: must not be empty", i)
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
	return nil
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
	if t.CertFile != "" || t.KeyFile != "" {
		if t.CertFile == "" || t.KeyFile == "" {
			return errors.New("cert_file and key_file must both be set for mutual TLS")
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
	default:
		return fmt.Errorf("unsupported mechanism %q (use none, plain, scram-sha-256, scram-sha-512)", s.Mechanism)
	}
	return nil
}

func (b *Bridge) validate(clusters map[string]Cluster) error {
	if strings.TrimSpace(b.Name) == "" {
		return errors.New("name is required")
	}
	if err := b.From.validate("from", clusters); err != nil {
		return err
	}
	if err := b.To.validate("to", clusters); err != nil {
		return err
	}
	if b.From.Cluster == b.To.Cluster && b.From.Topic == b.To.Topic {
		return errors.New("from and to cannot be the same cluster and topic")
	}
	return nil
}

func (t *BridgeTarget) validate(role string, clusters map[string]Cluster) error {
	if strings.TrimSpace(t.Cluster) == "" {
		return fmt.Errorf("%s.cluster is required", role)
	}
	if _, ok := clusters[t.Cluster]; !ok {
		return fmt.Errorf("%s.cluster %q is not defined under clusters", role, t.Cluster)
	}
	if strings.TrimSpace(t.Topic) == "" {
		return fmt.Errorf("%s.topic is required", role)
	}
	return nil
}

func (m *Metrics) validate() error {
	if err := validateMetricsExtraLabels(m.ExtraLabels); err != nil {
		return fmt.Errorf("extra_labels: %w", err)
	}
	if !m.MetricsEnabled() {
		return nil
	}
	addr := strings.TrimSpace(m.ListenAddr)
	if addr == "" {
		return errors.New("listen_addr is required when metrics are enabled (e.g. \":9090\")")
	}
	if _, err := net.ResolveTCPAddr("tcp", addr); err != nil {
		return fmt.Errorf("listen_addr: %w", err)
	}
	return nil
}

var promLabelNameRE = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

var metricVariableLabels = map[string]struct{}{
	"bridge": {}, "from_cluster": {}, "from_topic": {}, "to_cluster": {}, "to_topic": {},
	"stage": {}, "cluster": {}, "tls_version": {}, "le": {}, "quantile": {},
}

func validateMetricsExtraLabels(labels map[string]string) error {
	for k, v := range labels {
		name := strings.TrimSpace(k)
		if name == "" {
			return errors.New("label name must not be empty")
		}
		if !promLabelNameRE.MatchString(name) {
			return fmt.Errorf("label %q: invalid Prometheus label name", k)
		}
		if _, conflict := metricVariableLabels[name]; conflict {
			return fmt.Errorf("label %q conflicts with built-in metric variable labels", k)
		}
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("label %q: value must not be empty", k)
		}
	}
	return nil
}

// MetricsEnabled returns whether the metrics HTTP endpoint and collectors are active.
func (m *Metrics) MetricsEnabled() bool {
	if m == nil || m.Enable == nil {
		return true
	}
	return *m.Enable
}

// GroupForward reports whether the forward counter family is enabled.
func (g *MetricGroups) GroupForward() bool {
	if g == nil || g.Forward == nil {
		return true
	}
	return *g.Forward
}

// GroupErrors reports whether the errors counter family is enabled.
func (g *MetricGroups) GroupErrors() bool {
	if g == nil || g.Errors == nil {
		return true
	}
	return *g.Errors
}

// GroupLatency reports whether the latency histogram family is enabled.
func (g *MetricGroups) GroupLatency() bool {
	if g == nil || g.Latency == nil {
		return true
	}
	return *g.Latency
}

// GroupGolang reports whether Go runtime and build info collectors are registered.
func (g *MetricGroups) GroupGolang() bool {
	if g == nil || g.Golang == nil {
		return true
	}
	return *g.Golang
}

// GroupProcess reports whether the process collector is registered.
func (g *MetricGroups) GroupProcess() bool {
	if g == nil || g.Process == nil {
		return true
	}
	return *g.Process
}

// GroupKafka reports whether franz-go broker wire metrics (hooks) are enabled.
func (g *MetricGroups) GroupKafka() bool {
	if g == nil || g.Kafka == nil {
		return true
	}
	return *g.Kafka
}

// GroupTLS reports whether TLS handshake and certificate metrics from broker dials are enabled.
func (g *MetricGroups) GroupTLS() bool {
	if g == nil || g.TLS == nil {
		return true
	}
	return *g.TLS
}

// GroupTCP reports whether Linux TCP namespace metrics from /proc are exposed (no-op on non-Linux).
func (g *MetricGroups) GroupTCP() bool {
	if g == nil || g.TCP == nil {
		return true
	}
	return *g.TCP
}

func (l *Logging) validate() error {
	level := strings.ToLower(strings.TrimSpace(l.Level))
	switch level {
	case "trace", "debug", "info", "warn", "error", "fatal":
	default:
		return fmt.Errorf("level: unsupported %q (use trace, debug, info, warn, error, fatal)", l.Level)
	}
	format := strings.ToLower(strings.TrimSpace(l.Format))
	switch format {
	case "json", "logfmt":
	default:
		return fmt.Errorf("format: unsupported %q (use json, logfmt)", l.Format)
	}
	stream := strings.ToLower(strings.TrimSpace(l.Stream))
	switch stream {
	case "stdout", "stderr", "file":
	default:
		return fmt.Errorf("stream: unsupported %q (use stdout, stderr, file)", l.Stream)
	}
	if stream == "file" {
		if strings.TrimSpace(l.FilePath) == "" {
			return errors.New("file_path is required when stream is file")
		}
	}
	for k, v := range l.ExtraFields {
		name := strings.TrimSpace(k)
		if name == "" {
			return errors.New("extra_fields: key must not be empty")
		}
		if !promLabelNameRE.MatchString(name) {
			return fmt.Errorf("extra_fields[%q]: invalid key", k)
		}
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("extra_fields[%q]: value must not be empty", k)
		}
	}
	return nil
}

func (l *Logging) LevelKey() string {
	return strings.ToLower(strings.TrimSpace(l.Level))
}

func (l *Logging) FormatKey() string {
	return strings.ToLower(strings.TrimSpace(l.Format))
}

func (l *Logging) StreamKey() string {
	return strings.ToLower(strings.TrimSpace(l.Stream))
}

// EffectiveConsumerGroup returns the consumer group for this bridge.
func (b *Bridge) EffectiveConsumerGroup() string {
	if strings.TrimSpace(b.ConsumerGroup) != "" {
		return strings.TrimSpace(b.ConsumerGroup)
	}
	return "bifrost-" + sanitizeName(b.Name)
}

func sanitizeName(s string) string {
	repl := strings.NewReplacer(" ", "-", "_", "-")
	s = strings.ToLower(strings.TrimSpace(repl.Replace(s)))
	var b strings.Builder
	for _, r := range s {
		switch {
		case unicode.IsLetter(r) || unicode.IsDigit(r):
			b.WriteRune(r)
		case r == '-' || r == '.':
			b.WriteRune(r)
		default:
			b.WriteByte('-')
		}
	}
	out := strings.Trim(b.String(), "-.")
	for strings.Contains(out, "--") {
		out = strings.ReplaceAll(out, "--", "-")
	}
	if out == "" {
		return "default"
	}
	return out
}
