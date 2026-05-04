package config

import (
	"fmt"
	"strings"
)

func (c *Config) normalize() error {
	if c == nil {
		return nil
	}

	normalizedClusters := make(map[string]Cluster, len(c.Clusters))
	for name, cluster := range c.Clusters {
		cluster.normalize()
		normalizedName := strings.TrimSpace(name)
		if _, exists := normalizedClusters[normalizedName]; exists {
			return fmt.Errorf("clusters: duplicate name %q after normalization", normalizedName)
		}
		normalizedClusters[normalizedName] = cluster
	}
	c.Clusters = normalizedClusters

	for i := range c.Bridges {
		if err := c.Bridges[i].normalize(); err != nil {
			return err
		}
	}
	if err := c.Metrics.normalize(); err != nil {
		return err
	}
	if err := normalizeStringMap(c.Metrics.EffectiveExtraLabels(), "metrics.labels.extra"); err != nil {
		return err
	}
	if err := c.Logging.normalize(); err != nil {
		return err
	}
	return normalizeStringMap(c.Logging.EffectiveExtraFields(), "logging.fields.extra")
}

func (b *Bridge) normalize() error {
	b.Name = strings.TrimSpace(b.Name)
	b.ConsumerGroup = strings.TrimSpace(b.ConsumerGroup)
	b.From.normalize()
	b.To.normalize()
	return b.mergeLegacyExtraHeaders()
}

func (t *BridgeTarget) normalize() {
	t.Cluster = strings.TrimSpace(t.Cluster)
	t.Topic = strings.TrimSpace(t.Topic)
}

func (m *Metrics) normalize() error {
	if m != nil {
		m.ListenAddr = strings.TrimSpace(m.ListenAddr)
	}
	return m.mergeLegacyExtraLabels()
}

func (l *Logging) normalize() error {
	if l != nil {
		l.Level = strings.TrimSpace(l.Level)
		l.Format = strings.TrimSpace(l.Format)
		l.Stream = strings.TrimSpace(l.Stream)
		l.PeriodicStatsInterval = strings.TrimSpace(l.PeriodicStatsInterval)
	}
	return l.mergeLegacyExtraFields()
}

func (c *Cluster) normalize() {
	for i := range c.Brokers {
		c.Brokers[i] = strings.TrimSpace(c.Brokers[i])
	}
	c.TLS.CAFile = strings.TrimSpace(c.TLS.CAFile)
	c.TLS.CertFile = strings.TrimSpace(c.TLS.CertFile)
	c.TLS.KeyFile = strings.TrimSpace(c.TLS.KeyFile)

	c.SASL.Mechanism = strings.TrimSpace(c.SASL.Mechanism)
	c.SASL.Username = strings.TrimSpace(c.SASL.Username)
	c.SASL.Password = strings.TrimSpace(c.SASL.Password)

	c.Client.ClientID = strings.TrimSpace(c.Client.ClientID)
	c.Client.DialTimeout = strings.TrimSpace(c.Client.DialTimeout)
	c.Client.RequestTimeoutOverhead = strings.TrimSpace(c.Client.RequestTimeoutOverhead)

	c.Consumer.FetchMaxWait = strings.TrimSpace(c.Consumer.FetchMaxWait)
	c.Consumer.SessionTimeout = strings.TrimSpace(c.Consumer.SessionTimeout)
	c.Consumer.HeartbeatInterval = strings.TrimSpace(c.Consumer.HeartbeatInterval)
	c.Consumer.RebalanceTimeout = strings.TrimSpace(c.Consumer.RebalanceTimeout)
	c.Consumer.IsolationLevel = strings.TrimSpace(c.Consumer.IsolationLevel)
	c.Consumer.CommitRetry.MinBackoff = strings.TrimSpace(c.Consumer.CommitRetry.MinBackoff)
	c.Consumer.CommitRetry.MaxBackoff = strings.TrimSpace(c.Consumer.CommitRetry.MaxBackoff)
	c.Consumer.CommitRetry.Jitter = strings.TrimSpace(c.Consumer.CommitRetry.Jitter)

	c.Producer.RequiredAcks = strings.TrimSpace(c.Producer.RequiredAcks)
	c.Producer.BatchCompression = strings.TrimSpace(c.Producer.BatchCompression)
	c.Producer.Linger = strings.TrimSpace(c.Producer.Linger)
	c.Producer.ProduceRequestTimeout = strings.TrimSpace(c.Producer.ProduceRequestTimeout)
	c.Producer.Retry.MinBackoff = strings.TrimSpace(c.Producer.Retry.MinBackoff)
	c.Producer.Retry.MaxBackoff = strings.TrimSpace(c.Producer.Retry.MaxBackoff)
	c.Producer.Retry.Jitter = strings.TrimSpace(c.Producer.Retry.Jitter)
}

func normalizeStringMap(m map[string]string, field string) error {
	if len(m) == 0 {
		return nil
	}
	normalized := make(map[string]string, len(m))
	for key, value := range m {
		normalizedKey := strings.TrimSpace(key)
		if _, exists := normalized[normalizedKey]; exists {
			return fmt.Errorf("%s: duplicate key %q after normalization", field, normalizedKey)
		}
		normalized[normalizedKey] = strings.TrimSpace(value)
	}
	for key := range m {
		delete(m, key)
	}
	for key, value := range normalized {
		m[key] = value
	}
	return nil
}
