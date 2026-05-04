package config

import (
	"fmt"
	"maps"
	"strings"
)

// BridgeHeaders configures Kafka headers attached to records produced by a bridge.
type BridgeHeaders struct {
	// Extra are optional string headers (sorted when converted to records); keys must not use the
	// bifrost.* prefix (reserved for the relay).
	Extra map[string]string `yaml:"extra,omitempty"`
	// Source controls bifrost source metadata (course hash and optional structured headers).
	Source BridgeHeadersSource `yaml:"source,omitempty"`
	// Propagate when true (default) copies headers from each consumed record onto the produced record.
	Propagate *bool `yaml:"propagate,omitempty"`
}

// BridgeHeadersSource configures relay-owned source metadata headers.
type BridgeHeadersSource struct {
	// Enabled defaults to true; when false no bifrost course/source headers are added.
	Enabled *bool `yaml:"enabled,omitempty"`
	// Format is "compact" (default) or "verbose". Verbose adds bifrost.source.* after bifrost.course.hash.
	Format string `yaml:"format,omitempty"`
}

func (b *Bridge) applyHeadersDefaults() {
	if b == nil {
		return
	}
	if b.Headers == nil {
		b.Headers = &BridgeHeaders{}
	}
}

func (b *Bridge) mergeLegacyExtraHeaders() error {
	if b == nil || len(b.ExtraHeaders) == 0 {
		return nil
	}
	b.applyHeadersDefaults()
	if len(b.Headers.Extra) == 0 {
		b.Headers.Extra = maps.Clone(b.ExtraHeaders)
		return nil
	}
	if maps.Equal(trimStringMapKeysValues(b.Headers.Extra), trimStringMapKeysValues(b.ExtraHeaders)) {
		return nil
	}
	return fmt.Errorf("bridge %q: extra_headers and headers.extra both set with different entries", b.Name)
}

func trimStringMapKeysValues(m map[string]string) map[string]string {
	if len(m) == 0 {
		return nil
	}
	out := make(map[string]string, len(m))
	for k, v := range m {
		ks := strings.TrimSpace(k)
		out[ks] = strings.TrimSpace(v)
	}
	return out
}

// EffectiveHeadersExtra returns the configured extra header map (may be nil).
func (b *Bridge) EffectiveHeadersExtra() map[string]string {
	if b == nil || b.Headers == nil {
		return nil
	}
	return b.Headers.Extra
}

// SourceHeadersEnabled reports whether bifrost course / source headers are attached (default true).
func (b *Bridge) SourceHeadersEnabled() bool {
	if b == nil || b.Headers == nil || b.Headers.Source.Enabled == nil {
		return true
	}
	return *b.Headers.Source.Enabled
}

// SourceHeadersVerbose reports whether structured bifrost.source.* headers are added after the course hash (default false / compact).
func (b *Bridge) SourceHeadersVerbose() bool {
	if b == nil || b.Headers == nil {
		return false
	}
	return strings.EqualFold(strings.TrimSpace(b.Headers.Source.Format), "verbose")
}

// PropagateRecordHeaders reports whether consumed record headers are copied to produced records (default true).
func (b *Bridge) PropagateRecordHeaders() bool {
	if b == nil || b.Headers == nil || b.Headers.Propagate == nil {
		return true
	}
	return *b.Headers.Propagate
}

func validateBridgeHeaders(b *Bridge) error {
	if b == nil {
		return nil
	}
	if err := validateHeadersExtra(b.Headers, b.Name); err != nil {
		return err
	}
	if b.Headers != nil {
		f := strings.ToLower(strings.TrimSpace(b.Headers.Source.Format))
		if f != "" && f != "compact" && f != "verbose" {
			return fmt.Errorf("bridge %q: headers.source.format must be compact or verbose", b.Name)
		}
	}
	return nil
}

func validateHeadersExtra(hdr *BridgeHeaders, bridgeName string) error {
	if hdr == nil || len(hdr.Extra) == 0 {
		return nil
	}
	for k := range hdr.Extra {
		key := strings.TrimSpace(k)
		if key == "" {
			return fmt.Errorf("bridge %q: headers.extra: empty key", bridgeName)
		}
		if strings.HasPrefix(key, bifrostHeaderPrefix) {
			return fmt.Errorf("bridge %q: headers.extra: key %q must not use the %q prefix (reserved for bifrost)", bridgeName, key, bifrostHeaderPrefix)
		}
	}
	return nil
}
