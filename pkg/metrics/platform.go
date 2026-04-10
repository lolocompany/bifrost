package metrics

import (
	"fmt"

	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// RegisterPlatformCollectors registers standard Go, build, and process collectors when enabled in cfg.
// Go projects conventionally expose these as go_* and process_* collector families.
func RegisterPlatformCollectors(reg prometheus.Registerer, g bifrostconfig.MetricGroups) error {
	if g.GroupGolang() || g.GroupProcess() {
		if g.GroupGolang() {
			if err := reg.Register(collectors.NewBuildInfoCollector()); err != nil {
				return fmt.Errorf("register build info collector: %w", err)
			}
			if err := reg.Register(collectors.NewGoCollector()); err != nil {
				return fmt.Errorf("register go collector: %w", err)
			}
		}
		if g.GroupProcess() {
			if err := reg.Register(collectors.NewProcessCollector(collectors.ProcessCollectorOpts{})); err != nil {
				return fmt.Errorf("register process collector: %w", err)
			}
		}
	}
	// TCP metrics are registered from broker hooks in broker_prom.go (cross-platform).
	return nil
}
