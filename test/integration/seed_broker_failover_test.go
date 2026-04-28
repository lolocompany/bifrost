package integration_test

import (
	"context"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/test/integration/testutil/artifacts"
	kafkautil "github.com/lolocompany/bifrost/test/integration/testutil/kafka"
	metricutil "github.com/lolocompany/bifrost/test/integration/testutil/metrics"
	processutil "github.com/lolocompany/bifrost/test/integration/testutil/process"
	"github.com/lolocompany/bifrost/test/integration/testutil/scenario"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TestClusterBrokerFailover_SecondSeedReachable validates end-to-end failover through the CLI:
// first seed is unreachable, second seed works.
func TestClusterBrokerFailover_SecondSeedReachable(t *testing.T) {
	requireIntegration(t)
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	brokers := kafkautil.StartCluster(t, ctx, kafkautil.ProviderRedpanda)
	seed := brokers[0]
	withBadFirst := []string{"127.0.0.1:1", seed}

	pump := kafkautil.NewClient(t, brokers)
	defer pump.Close()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "it.failover.from." + suffix
	toTopic := "it.failover.to." + suffix
	kafkautil.MustCreateTopic(t, ctx, pump, fromTopic, 1)
	kafkautil.MustCreateTopic(t, ctx, pump, toTopic, 1)
	want := []byte("failover-ok")
	kafkautil.ProduceSync(t, ctx, pump, &kgo.Record{Topic: fromTopic, Value: want})

	metricsAddr, err := processutil.FreeTCPAddr()
	if err != nil {
		t.Fatalf("metrics addr: %v", err)
	}
	tmpl, err := processutil.ReadFile(filepath.Join("fixtures", "single_bridge.yaml.tmpl"))
	if err != nil {
		t.Fatalf("fixture read: %v", err)
	}
	cfg, err := scenario.RenderConfig(string(tmpl), scenario.ConfigData{
		FromTopic:      fromTopic,
		ToTopic:        toTopic,
		FromCluster:    "a",
		ToCluster:      "b",
		BrokersFrom:    withBadFirst,
		BrokersTo:      withBadFirst,
		MetricsAddr:    metricsAddr,
		BridgeName:     "failover",
		ConsumerGroup:  "failover-cg-" + suffix,
		BatchSize:      1,
		Replicas:       1,
		HasOverrideKey: false,
	})
	if err != nil {
		t.Fatalf("render config: %v", err)
	}
	art := artifacts.New(t, "integration")
	if err := art.WriteConfig(cfg); err != nil {
		t.Fatalf("write config: %v", err)
	}
	proc, err := processutil.Start(ctx, art, metricsAddr)
	if err != nil {
		t.Fatalf("start process: %v", err)
	}
	t.Cleanup(func() {
		if err := proc.Stop(); err != nil {
			t.Errorf("stop bifrost: %v", err)
		}
	})
	if err := proc.WaitReady(ctx); err != nil {
		t.Fatalf("wait ready: %v", err)
	}

	verify := kafkautil.NewClient(t, brokers, kgo.ConsumeTopics(toTopic), kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()))
	defer verify.Close()
	records, err := kafkautil.WaitForRecords(ctx, verify, toTopic, 1, func(r *kgo.Record) bool {
		return string(r.Value) == string(want)
	})
	if err != nil || len(records) != 1 {
		t.Fatalf("failover relay assertion: records=%d err=%v", len(records), err)
	}
	if _, err := metricutil.WaitContains("http://"+metricsAddr+"/metrics", 10*time.Second, "bifrost_relay_messages_total"); err != nil {
		t.Fatalf("metrics assert: %v", err)
	}
}
