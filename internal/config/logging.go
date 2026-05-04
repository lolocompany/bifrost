package config

import (
	"errors"
	"fmt"
	"maps"
	"strings"
	"time"
)

// Logging configures process logging (slog).
type Logging struct {
	Level  string `yaml:"level"`  // trace, debug, info, warn, error, fatal
	Format string `yaml:"format"` // json (default) or logfmt
	Stream string `yaml:"stream"` // stdout or stderr
	// Fields holds optional attributes included on every log line (nested extra map).
	Fields *LoggingFields `yaml:"fields,omitempty"`
	// ExtraFields is deprecated: use fields.extra. If both are set they must agree after normalization.
	ExtraFields map[string]string `yaml:"extra_fields,omitempty"`
	// PeriodicStatsInterval is how often each bridge logs relay stats (messages/errors since the
	// previous log) at info level. Go duration syntax (e.g. "1m", "30s"). Empty defaults to "5m".
	// Use "0" or "0s" to disable.
	PeriodicStatsInterval string `yaml:"periodic_stats_interval"`
}

// LoggingFields configures slog attributes always attached to log records.
type LoggingFields struct {
	Extra map[string]string `yaml:"extra,omitempty"`
}

func (l *Logging) ApplyDefaults() {
	if strings.TrimSpace(l.Level) == "" {
		l.Level = "info"
	}
	if strings.TrimSpace(l.Format) == "" {
		l.Format = "json"
	}
	if strings.TrimSpace(l.Stream) == "" {
		l.Stream = "stdout"
	}
	if strings.TrimSpace(l.PeriodicStatsInterval) == "" {
		l.PeriodicStatsInterval = "5m"
	}
}

func (l *Logging) mergeLegacyExtraFields() error {
	if l == nil || len(l.ExtraFields) == 0 {
		return nil
	}
	if l.Fields == nil {
		l.Fields = &LoggingFields{}
	}
	if len(l.Fields.Extra) == 0 {
		l.Fields.Extra = maps.Clone(l.ExtraFields)
		return nil
	}
	if maps.Equal(trimStringMapKeysValues(l.Fields.Extra), trimStringMapKeysValues(l.ExtraFields)) {
		return nil
	}
	return fmt.Errorf("logging: extra_fields and logging.fields.extra both set with different entries")
}

// EffectiveExtraFields returns attributes from logging.fields.extra (may be nil).
func (l *Logging) EffectiveExtraFields() map[string]string {
	if l == nil || l.Fields == nil {
		return nil
	}
	return l.Fields.Extra
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
	case "stdout", "stderr":
	default:
		return fmt.Errorf("stream: unsupported %q (use stdout, stderr)", l.Stream)
	}
	for k, v := range l.EffectiveExtraFields() {
		name := strings.TrimSpace(k)
		if name == "" {
			return errors.New("logging.fields.extra: key must not be empty")
		}
		if !promLabelNameRE.MatchString(name) {
			return fmt.Errorf("logging.fields.extra[%q]: invalid key", k)
		}
		if strings.TrimSpace(v) == "" {
			return fmt.Errorf("logging.fields.extra[%q]: value must not be empty", k)
		}
	}
	if _, err := l.ParsePeriodicStatsInterval(); err != nil {
		return fmt.Errorf("periodic_stats_interval: %w", err)
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

// ParsePeriodicStatsInterval returns the interval for periodic per-bridge relay stats logs at info
// level. A duration of 0 means disabled.
func (l *Logging) ParsePeriodicStatsInterval() (time.Duration, error) {
	s := strings.TrimSpace(l.PeriodicStatsInterval)
	d, err := time.ParseDuration(s)
	if err != nil {
		return 0, fmt.Errorf("parse duration: %w", err)
	}
	if d < 0 {
		return 0, fmt.Errorf("must not be negative")
	}
	return d, nil
}
