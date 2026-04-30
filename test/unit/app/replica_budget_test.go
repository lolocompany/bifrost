package app_test

import (
	"testing"

	"github.com/lolocompany/bifrost/internal/app"
)

func TestPlanReplicaCounts_explicitOnly(t *testing.T) {
	t.Parallel()
	snap := app.SystemSnapshot{GOMAXPROCS: 8, AvailableBytes: 16 << 30}
	cfg := []int{3, 7}
	part := []int{0, 0}
	got, note, err := app.PlanReplicaCounts(cfg, part, snap)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 3 || got[1] != 7 {
		t.Fatalf("got %#v", got)
	}
	if note.ExplicitReplicaSum != 10 {
		t.Fatalf("explicit sum: %d", note.ExplicitReplicaSum)
	}
}

func TestPlanReplicaCounts_autoFitsUnderGlobalCap(t *testing.T) {
	t.Parallel()
	snap := app.SystemSnapshot{GOMAXPROCS: 8, AvailableBytes: 64 << 30}
	cfg := []int{0, 0}
	// Per-bridge cap at GOMAXPROCS=8 is min(4096, max(4, 64)) = 64
	part := []int{30, 40}
	got, _, err := app.PlanReplicaCounts(cfg, part, snap)
	if err != nil {
		t.Fatal(err)
	}
	if got[0] != 30 || got[1] != 40 {
		t.Fatalf("got %#v want [30 40]", got)
	}
	if sumInt(got) != 70 {
		t.Fatalf("sum %d", sumInt(got))
	}
}

func TestPlanReplicaCounts_autoFairShareWhenOverCap(t *testing.T) {
	t.Parallel()
	snap := app.SystemSnapshot{GOMAXPROCS: 4, AvailableBytes: 128 << 30}
	n := 20
	cfg := make([]int, n)
	part := make([]int, n)
	for i := range cfg {
		cfg[i] = 0
		part[i] = 200 // per-bridge raw min(200, 32) = 32 → would sum 640 without sharing
	}
	got, note, err := app.PlanReplicaCounts(cfg, part, snap)
	if err != nil {
		t.Fatal(err)
	}
	total := sumInt(got)
	if total != note.GlobalSoftCap {
		t.Fatalf("sum %d != global soft cap %d", total, note.GlobalSoftCap)
	}
	if total > note.GlobalSoftCap {
		t.Fatal("sum exceeds cap")
	}
	for _, g := range got {
		if g < 1 {
			t.Fatalf("bridge below minimum: %#v", got)
		}
	}
}

func sumInt(xs []int) int {
	s := 0
	for _, x := range xs {
		s += x
	}
	return s
}

func TestComputeGlobalRelayCap_atLeastBridgeCount(t *testing.T) {
	t.Parallel()
	if g := app.ComputeGlobalRelayCap(5, app.SystemSnapshot{GOMAXPROCS: 1, AvailableBytes: 0}); g < 5 {
		t.Fatalf("got %d", g)
	}
}
