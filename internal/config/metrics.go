package config

import (
	"errors"
	"fmt"
	"net"
	"regexp"
	"strings"
)

// Metrics configures the Prometheus scrape endpoint and which series to expose.
type Metrics struct {
	Enable      *bool             `yaml:"enabled"`     // default true when omitted
	ListenAddr  string            `yaml:"listen_addr"` // default :9090 when metrics are enabled and omitted/blank
	Groups      MetricGroups      `yaml:"groups"`
	ExtraLabels map[string]string `yaml:"extra_labels"` // added to all bifrost metrics as constant labels
}

// MetricGroups toggles optional metric families. Omitted or nil fields default to enabled.
// bifrost_relay_* bridge metrics are always registered when the metrics endpoint is enabled; they are not
// configurable. Other application families use the bifrost_ namespace (bifrost_kafka_, bifrost_tls_,
// bifrost_tcp_). The golang/process groups expose standard client_golang collector names (go_* and
// process_*). See internal/observability/metrics for details.
type MetricGroups struct {
	Golang  *bool `yaml:"golang"`  // Go runtime + build info collectors
	Process *bool `yaml:"process"` // process (CPU/mem/fd) collector
	Kafka   *bool `yaml:"kafka"`   // franz-go broker wire / throttle metrics
	TLS     *bool `yaml:"tls"`     // TLS handshake + peer cert from broker connections
	TCP     *bool `yaml:"tcp"`     // TCP broker socket metrics from franz-go hooks (cross-platform)
}

func (m *Metrics) ApplyDefaults() {
	if !m.MetricsEnabled() {
		return
	}
	if strings.TrimSpace(m.ListenAddr) == "" {
		m.ListenAddr = ":9090"
	}
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

var builtInMetricVariableLabels = map[string]struct{}{
	"bridge": {}, "from_kafka_cluster": {}, "from_topic": {}, "to_kafka_cluster": {}, "to_topic": {},
	"stage": {}, "state": {}, "kafka_cluster": {}, "tls_version": {}, "le": {}, "quantile": {},
}

// MetricVariableLabels returns a copy of built-in variable labels used by bifrost metric vectors.
func MetricVariableLabels() map[string]struct{} {
	out := make(map[string]struct{}, len(builtInMetricVariableLabels))
	for k := range builtInMetricVariableLabels {
		out[k] = struct{}{}
	}
	return out
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
		if _, conflict := builtInMetricVariableLabels[name]; conflict {
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

// GroupTCP reports whether TCP broker socket metrics from franz-go hooks are enabled.
func (g *MetricGroups) GroupTCP() bool {
	if g == nil || g.TCP == nil {
		return true
	}
	return *g.TCP
}
