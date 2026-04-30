package app_test

import (
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/internal/app"
	"github.com/lolocompany/bifrost/internal/config"
)

func TestValidatePreservedPartitionCounts_WithOverridePartition(t *testing.T) {
	t.Parallel()
	override := int32(2)
	bridgeCfg := config.Bridge{
		Name:              "b1",
		OverridePartition: &override,
		From:              config.BridgeTarget{Cluster: "a", Topic: "src"},
		To:                config.BridgeTarget{Cluster: "b", Topic: "dst"},
	}
	if err := app.ValidatePreservedPartitionCounts(bridgeCfg, 10, 3); err != nil {
		t.Fatalf("expected success, got %v", err)
	}
	if err := app.ValidatePreservedPartitionCounts(bridgeCfg, 10, 2); err == nil {
		t.Fatal("expected error when override partition out of destination range")
	}
}

func TestValidatePreservedPartitionCounts_PreserveMode(t *testing.T) {
	t.Parallel()
	bridgeCfg := config.Bridge{
		Name: "b2",
		From: config.BridgeTarget{Cluster: "a", Topic: "src"},
		To:   config.BridgeTarget{Cluster: "b", Topic: "dst"},
	}
	if err := app.ValidatePreservedPartitionCounts(bridgeCfg, 6, 6); err != nil {
		t.Fatalf("expected equal partitions to pass, got %v", err)
	}
	err := app.ValidatePreservedPartitionCounts(bridgeCfg, 6, 5)
	if err == nil {
		t.Fatal("expected error when destination has fewer partitions")
	}
	if !strings.Contains(err.Error(), "at least 6 partitions") {
		t.Fatalf("unexpected error: %v", err)
	}
}
