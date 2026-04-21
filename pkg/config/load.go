package config

import (
	"bytes"
	"fmt"
	"os"

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

// Parse unmarshals YAML bytes into Config and validates it.
func Parse(data []byte) (*Config, error) {
	var cfg Config
	dec := yaml.NewDecoder(bytes.NewReader(data))
	dec.KnownFields(true)
	if err := dec.Decode(&cfg); err != nil {
		return nil, fmt.Errorf("parse yaml: %w", err)
	}
	if err := cfg.normalize(); err != nil {
		return nil, err
	}
	cfg.ApplyDefaults()
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

