package config

import (
	"errors"
	"fmt"
	"strings"
)

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
