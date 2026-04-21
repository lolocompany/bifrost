// Package config loads and validates bridge YAML configuration.
//
// Parse and Load return an error when required fields are missing or invalid. Defaults are applied
// centrally in this package via Config.ApplyDefaults and nested struct methods.
// MustParse and MustLoad panic instead of returning an error—use only when startup must abort on bad config.
package config

// Config is the top-level configuration for bifrost.
type Config struct {
	Clusters map[string]Cluster `yaml:"clusters"`
	Bridges  []Bridge           `yaml:"bridges"`
	Metrics  Metrics            `yaml:"metrics"`
	Logging  Logging            `yaml:"logging"`
}

func (c *Config) ApplyDefaults() {
	if c == nil {
		return
	}
	c.Logging.ApplyDefaults()
	c.Metrics.ApplyDefaults()
	for name, cluster := range c.Clusters {
		cluster.ApplyDefaults()
		c.Clusters[name] = cluster
	}
	for i := range c.Bridges {
		c.Bridges[i].ApplyDefaults()
	}
}
