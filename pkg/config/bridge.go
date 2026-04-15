package config

import (
	"errors"
	"fmt"
	"strings"
	"unicode"
)

// MaxReplicas is the upper bound for an explicit [Bridge.Replicas] value (relay goroutines per bridge).
const MaxReplicas = 4096

// Bridge defines one directional relay between two clusters and topics (from → to).
type Bridge struct {
	Name string       `yaml:"name"`
	From BridgeTarget `yaml:"from"`
	To   BridgeTarget `yaml:"to"`
	// Replicas is relay goroutines for this bridge. Use 0 or omit for automatic sizing from the
	// source topic partition count (after topic ensure), subject to process-wide heuristics in
	// pkg/bifrost. If set (>0), that many goroutines run; each uses the same to-side producer and
	// its own from-side consumer; consumers share EffectiveConsumerGroup() so partitions are split
	// across group members.
	Replicas      int               `yaml:"replicas"`
	ConsumerGroup string            `yaml:"consumer_group"`
	ExtraHeaders  map[string]string `yaml:"extra_headers,omitempty"`
}

// BridgeTarget references a cluster name (key in clusters) and a single topic.
type BridgeTarget struct {
	Cluster string `yaml:"cluster"`
	Topic   string `yaml:"topic"`
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
	if err := validateExtraHeaders(b.ExtraHeaders); err != nil {
		return err
	}
	if b.Replicas < 0 {
		return errors.New("replicas must be 0 (automatic sizing) or at least 1")
	}
	if b.Replicas > MaxReplicas {
		return fmt.Errorf("replicas must be at most %d", MaxReplicas)
	}
	return nil
}

// bifrostHeaderPrefix is reserved for headers set by bifrost (e.g. bifrost.source.*).
const bifrostHeaderPrefix = "bifrost."

func validateExtraHeaders(m map[string]string) error {
	if len(m) == 0 {
		return nil
	}
	for k := range m {
		key := strings.TrimSpace(k)
		if key == "" {
			return fmt.Errorf("extra_headers: empty key")
		}
		if strings.HasPrefix(key, bifrostHeaderPrefix) {
			return fmt.Errorf("extra_headers: key %q must not use the %q prefix (reserved for bifrost)", key, bifrostHeaderPrefix)
		}
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
