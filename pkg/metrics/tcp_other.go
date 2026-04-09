//go:build !linux

package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
)

// NewTCPNamespaceCollector is a no-op on non-Linux platforms (no /proc TCP stats).
func NewTCPNamespaceCollector() (prometheus.Collector, error) {
	return nil, nil
}
