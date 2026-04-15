package bifrost

import (
	"fmt"
	"math"
	"sort"

	"github.com/lolocompany/bifrost/pkg/config"
)

// ReplicaPlanNote captures how many bridge relay goroutines were chosen (for startup logs).
type ReplicaPlanNote struct {
	GlobalSoftCap         int
	PerBridgePartitionCap int
	ExplicitReplicaSum    int
	AutoRawTargetSum      int // sum of min(partitions, per-bridge cap) for auto bridges before sharing
	HeadroomForAuto       int // global soft cap minus explicit replica sum
}

// ComputeGlobalRelayCap returns a soft upper bound on total bridge.Run goroutines (sum across bridges),
// combining CPU, memory, and a hard ceiling.
func ComputeGlobalRelayCap(numBridges int, snap SystemSnapshot) int {
	if numBridges < 1 {
		return 0
	}
	c := snap.GOMAXPROCS
	if c < 1 {
		c = 1
	}
	cpuCap := max(numBridges, globalRelayCPUMultiplier*c)

	memCap := math.MaxInt
	if snap.AvailableBytes > 0 {
		usable := snap.AvailableBytes * memoryBudgetPercent / 100
		est := int(usable / uint64(bytesPerRelayReplicaEstimate))
		if est < numBridges {
			est = numBridges
		}
		memCap = est
	}

	total := min(cpuCap, memCap, globalRelayTotalHardCap)
	if total < numBridges {
		return numBridges
	}
	return total
}

// PerBridgePartitionCap limits how many replicas one auto-configured bridge will ask for from
// partition count alone (before global fair-sharing).
func PerBridgePartitionCap(snap SystemSnapshot) int {
	c := snap.GOMAXPROCS
	if c < 1 {
		c = 1
	}
	soft := perBridgeCPUMultiplier * c
	if soft < perBridgeReplicaFloor {
		soft = perBridgeReplicaFloor
	}
	return min(config.MaxReplicas, soft)
}

// PlanReplicaCounts combines explicit config.Replicas with auto targets from source topic partitions.
// configReplicas[i]==0 means auto; partitionCounts[i] must be source partitions in that case.
func PlanReplicaCounts(configReplicas []int, partitionCounts []int, snap SystemSnapshot) (effective []int, note ReplicaPlanNote, err error) {
	n := len(configReplicas)
	if len(partitionCounts) != n {
		return nil, note, fmt.Errorf("partitionCounts length %d != bridges %d", len(partitionCounts), n)
	}

	effective = make([]int, n)
	explicitSum := 0
	for i, cr := range configReplicas {
		if cr < 0 {
			return nil, note, fmt.Errorf("bridge[%d]: invalid config replicas", i)
		}
		if cr > 0 {
			effective[i] = cr
			explicitSum += cr
		}
	}

	globalCap := ComputeGlobalRelayCap(n, snap)
	note.GlobalSoftCap = globalCap
	note.ExplicitReplicaSum = explicitSum
	note.HeadroomForAuto = globalCap - explicitSum

	partCap := PerBridgePartitionCap(snap)
	note.PerBridgePartitionCap = partCap

	remaining := note.HeadroomForAuto

	var autoIdx []int
	var rawWeights []int

	for i, cr := range configReplicas {
		if cr > 0 {
			continue
		}
		p := partitionCounts[i]
		if p < 1 {
			return nil, note, fmt.Errorf("bridge[%d]: source topic has no partitions in metadata", i)
		}
		w := min(p, partCap)
		autoIdx = append(autoIdx, i)
		rawWeights = append(rawWeights, w)
		note.AutoRawTargetSum += w
	}

	if len(autoIdx) == 0 {
		return effective, note, nil
	}

	if remaining < len(autoIdx) {
		for _, idx := range autoIdx {
			effective[idx] = 1
		}
		return effective, note, nil
	}

	alloc := allocateAutoFromWeights(rawWeights, remaining)
	for j, idx := range autoIdx {
		effective[idx] = min(alloc[j], rawWeights[j])
	}
	return effective, note, nil
}

// allocateAutoFromWeights splits budget across len(weights) bridges. Each bridge gets at least one
// slot when budget >= len(weights). Extra slots are spread in proportion to (weights-1), capped by weights.
func allocateAutoFromWeights(weights []int, budget int) []int {
	n := len(weights)
	out := make([]int, n)
	if n == 0 {
		return out
	}
	if budget < n {
		for i := range out {
			out[i] = 1
		}
		return out
	}

	for i := range out {
		out[i] = 1
	}
	left := budget - n
	if left < 0 {
		return out
	}

	extraCap := make([]int, n)
	sumCap := 0
	for i, w := range weights {
		ex := w - 1
		if ex < 0 {
			ex = 0
		}
		extraCap[i] = ex
		sumCap += ex
	}
	if sumCap == 0 || left == 0 {
		return out
	}

	if left >= sumCap {
		copy(out, weights)
		return out
	}

	// Largest-remainder allocation of `left` across extraCap.
	exact := make([]float64, n)
	for i, ec := range extraCap {
		exact[i] = float64(left) * float64(ec) / float64(sumCap)
	}
	floor := make([]int, n)
	type rem struct {
		i int
		r float64
	}
	rems := make([]rem, n)
	sf := 0
	for i := range exact {
		fi := int(math.Floor(exact[i]))
		if fi < 0 {
			fi = 0
		}
		if fi > extraCap[i] {
			fi = extraCap[i]
		}
		floor[i] = fi
		sf += fi
		rems[i] = rem{i, exact[i] - float64(fi)}
	}
	rest := left - sf
	sort.Slice(rems, func(a, b int) bool {
		if rems[a].r == rems[b].r {
			return rems[a].i < rems[b].i
		}
		return rems[a].r > rems[b].r
	})
	for k := 0; k < rest && k < len(rems); k++ {
		ix := rems[k].i
		if floor[ix] < extraCap[ix] {
			floor[ix]++
		}
	}

	for i := range out {
		out[i] = 1 + floor[i]
	}

	// Fix any drift from integer caps (rest might exceed distributable slots).
	for intSum(out) > budget {
		reduceOneTowardsWeight(out, weights)
	}
	for intSum(out) < budget {
		growTowardsWeight(out, weights)
	}
	return out
}

func intSum(xs []int) int {
	s := 0
	for _, x := range xs {
		s += x
	}
	return s
}

func reduceOneTowardsWeight(out, weights []int) {
	for i := len(out) - 1; i >= 0; i-- {
		if out[i] > 1 {
			out[i]--
			return
		}
	}
}

func growTowardsWeight(out, weights []int) {
	best := -1
	bestGap := -1
	for i, w := range weights {
		if out[i] < w {
			g := w - out[i]
			if g > bestGap {
				bestGap = g
				best = i
			}
		}
	}
	if best >= 0 {
		out[best]++
	}
}

const (
	globalRelayCPUMultiplier = 32
	perBridgeCPUMultiplier   = 8
	perBridgeReplicaFloor    = 4

	bytesPerRelayReplicaEstimate = 40 << 20
	memoryBudgetPercent          = 65

	globalRelayTotalHardCap = 16384
)
