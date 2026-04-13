package config

import (
	"bytes"
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

// Load reads and parses a YAML file from path.
func Load(path string) (*Config, error) {
	// Path is the operator-supplied config file (CLI flag / env), not untrusted URL input.
	data, err := os.ReadFile(path) // #nosec G304
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
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	cfg.applyDefaults()
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
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
	if strings.TrimSpace(c.Logging.PeriodicStatsInterval) == "" {
		c.Logging.PeriodicStatsInterval = "5m"
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
