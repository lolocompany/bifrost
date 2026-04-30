package metrics

import (
	"fmt"

	"github.com/lolocompany/bifrost/internal/config"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/collectors"
)

// RegisterRuntimeCollectors registers standard Go, build, and process collectors when enabled in cfg.
// Go projects conventionally expose these as go_* and process_* collector families.
func RegisterRuntimeCollectors(reg prometheus.Registerer, g config.MetricGroups) error {
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
	return nil
}
