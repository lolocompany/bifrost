package metrics

import (
	"fmt"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// RegisterPlatformCollectors registers standard Go, build, and process collectors when enabled in cfg.
// Go and process series are registered with the bifrost_ namespace (e.g. bifrost_go_*, bifrost_process_*).
func RegisterPlatformCollectors(reg prometheus.Registerer, g bifrostconfig.MetricGroups) error {
	if g.GroupGolang() || g.GroupProcess() {
		pref := prometheus.WrapRegistererWithPrefix("bifrost_", reg)
		if g.GroupGolang() {
			if err := pref.Register(collectors.NewBuildInfoCollector()); err != nil {
				return fmt.Errorf("register build info collector: %w", err)
			}
			if err := pref.Register(collectors.NewGoCollector()); err != nil {
				return fmt.Errorf("register go collector: %w", err)
			}
		}
		if g.GroupProcess() {
			if err := pref.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
				return fmt.Errorf("register process collector: %w", err)
			}
		}
	}
	if g.GroupTCP() {
		c, err := NewTCPNamespaceCollector()
		if err != nil {
			return fmt.Errorf("tcp namespace collector: %w", err)
		}
		if c != nil {
			if err := reg.Register(c); err != nil {
				return fmt.Errorf("register tcp namespace collector: %w", err)
			}
		}
	}
	return nil
}
