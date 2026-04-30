package app

import (
	"log/slog"
	"runtime"

	"github.com/shirou/gopsutil/v4/mem"
)

// SystemSnapshot captures host signals used when auto-scaling bridge replica counts (replicas omitted or 0).
// Budget heuristics combine:
//   - CPU: max(bridges, globalRelayCPUMultiplier×GOMAXPROCS) as a soft upper bound on total relays.
//   - Memory: ~memoryBudgetPercent of AvailableBytes divided by bytesPerRelayReplicaEstimate.
//   - A hard ceiling globalRelayTotalHardCap.
//
// Per-bridge auto targets use min(partitions, PerBridgePartitionCap) before fair-sharing.
type SystemSnapshot struct {
	GOMAXPROCS     int
	AvailableBytes uint64 // best-effort from OS; 0 means unknown (CPU-only heuristics apply).
}

// probeSystemSnapshot collects GOMAXPROCS and best-effort available memory for relay sizing.
func probeSystemSnapshot() SystemSnapshot {
	s := SystemSnapshot{
		GOMAXPROCS: runtime.GOMAXPROCS(0),
	}
	v, err := mem.VirtualMemory()
	if err != nil || v == nil {
		slog.Debug("memory probe unavailable for replica autoscale", "error", err)
		return s
	}
	s.AvailableBytes = v.Available
	return s
}
