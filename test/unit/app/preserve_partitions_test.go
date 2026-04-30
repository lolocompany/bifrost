package app_test

import (
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/internal/app"
	"github.com/lolocompany/bifrost/internal/config"
)

func TestValidatePreservedPartitionCounts(t *testing.T) {
	t.Run("override partition within destination", func(t *testing.T) {
		overridePartition := int32(1)
		bridgeCfg := config.Bridge{
			Name:              "east-to-west",
			OverridePartition: &overridePartition,
			From:              config.BridgeTarget{Cluster: "east", Topic: "input"},
			To:                config.BridgeTarget{Cluster: "west", Topic: "output"},
		}
		if err := app.ValidatePreservedPartitionCounts(bridgeCfg, 8, 4); err != nil {
			t.Fatalf("validatePreservedPartitionCounts: %v", err)
		}
	})

	t.Run("override partition too large", func(t *testing.T) {
		overridePartition := int32(4)
		bridgeCfg := config.Bridge{
			Name:              "east-to-west",
			OverridePartition: &overridePartition,
			From:              config.BridgeTarget{Cluster: "east", Topic: "input"},
			To:                config.BridgeTarget{Cluster: "west", Topic: "output"},
		}
		err := app.ValidatePreservedPartitionCounts(bridgeCfg, 8, 4)
		if err == nil {
			t.Fatal("validatePreservedPartitionCounts: expected error")
		}
		if !strings.Contains(err.Error(), "override_partition") {
			t.Fatalf("error = %v, want override_partition context", err)
		}
	})

	t.Run("destination large enough", func(t *testing.T) {
		bridgeCfg := config.Bridge{
			Name: "east-to-west",
			From: config.BridgeTarget{Cluster: "east", Topic: "input"},
			To:   config.BridgeTarget{Cluster: "west", Topic: "output"},
		}
		if err := app.ValidatePreservedPartitionCounts(bridgeCfg, 8, 8); err != nil {
			t.Fatalf("validatePreservedPartitionCounts: %v", err)
		}
	})

	t.Run("destination too small", func(t *testing.T) {
		bridgeCfg := config.Bridge{
			Name: "east-to-west",
			From: config.BridgeTarget{Cluster: "east", Topic: "input"},
			To:   config.BridgeTarget{Cluster: "west", Topic: "output"},
		}
		err := app.ValidatePreservedPartitionCounts(bridgeCfg, 8, 4)
		if err == nil {
			t.Fatal("validatePreservedPartitionCounts: expected error")
		}
		if !strings.Contains(err.Error(), "preserve source partitions") {
			t.Fatalf("error = %v, want preserve source partitions context", err)
		}
	})
}
