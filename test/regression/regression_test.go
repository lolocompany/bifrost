package regression_test

import (
	"context"
	"fmt"
	"math/rand"
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

func TestRegression_ConfigMatrix(t *testing.T) {
	kafkautil.RequireIntegration(t)
	matrix := []struct {
		name      string
		replicas  int
		batchSize int
	}{
		{"r1-b1", 1, 1},
		{"r2-b1", 2, 1},
		{"r2-b3", 2, 3},
	}
	for _, tc := range matrix {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			runRegressionCase(t, tc.name, tc.replicas, tc.batchSize, 5, 1)
		})
	}
}

func TestRegression_Randomized(t *testing.T) {
	kafkautil.RequireIntegration(t)
	seed := int64(20260422)
	r := rand.New(rand.NewSource(seed))
	for i := 0; i < 4; i++ {
		replicas := 1 + r.Intn(2)
		batchSize := 1 + r.Intn(3)
		inputs := 3 + r.Intn(4)
		partitions := int32(1 + r.Intn(3))
		name := fmt.Sprintf("seed-%d-case-%d-r%d-b%d-i%d-p%d", seed, i, replicas, batchSize, inputs, partitions)
		t.Run(name, func(t *testing.T) {
			runRegressionCase(t, name, replicas, batchSize, inputs, partitions)
		})
	}
}

func runRegressionCase(t *testing.T, name string, replicas, batchSize, inputCount int, partitions int32) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 90*time.Second)
	defer cancel()
	brokers := kafkautil.StartCluster(t, ctx, kafkautil.ProviderRedpanda)
	pump := kafkautil.NewClient(t, brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	defer pump.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "it.reg.from." + suffix
	toTopic := "it.reg.to." + suffix
	kafkautil.MustCreateTopic(t, ctx, pump, fromTopic, partitions)
	kafkautil.MustCreateTopic(t, ctx, pump, toTopic, partitions)
	for i := 0; i < inputCount; i++ {
		part := int32(i) % partitions
		kafkautil.ProduceSync(t, ctx, pump, &kgo.Record{
			Topic: fromTopic, Partition: part, Key: []byte(fmt.Sprintf("k-%d", i)), Value: []byte(fmt.Sprintf("v-%d", i)),
		})
	}

	metricsAddr, err := processutil.FreeTCPAddr()
	if err != nil {
		t.Fatalf("metrics addr: %v", err)
	}
	tmpl, err := processutil.ReadFile(filepath.Join("..", "integration", "fixtures", "single_bridge.yaml.tmpl"))
	if err != nil {
		t.Fatalf("fixture read: %v", err)
	}
	cfg, err := scenario.RenderConfig(string(tmpl), scenario.ConfigData{
		FromTopic:      fromTopic,
		ToTopic:        toTopic,
		FromCluster:    "a",
		ToCluster:      "b",
		BrokersFrom:    brokers,
		BrokersTo:      brokers,
		MetricsAddr:    metricsAddr,
		BridgeName:     "regression-" + name,
		ConsumerGroup:  "reg-cg-" + suffix,
		BatchSize:      batchSize,
		Replicas:       replicas,
		HasOverrideKey: false,
	})
	if err != nil {
		t.Fatalf("render config: %v", err)
	}
	art := artifacts.New(t, "regression")
	art.KeepOnOK = true
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
	records, err := kafkautil.WaitForRecords(ctx, verify, toTopic, inputCount, nil)
	if err != nil {
		t.Fatalf("records: %v", err)
	}
	if len(records) != inputCount {
		t.Fatalf("records=%d want=%d", len(records), inputCount)
	}
	if _, err := metricutil.WaitContains("http://"+metricsAddr+"/metrics", 10*time.Second,
		"bifrost_relay_messages_total",
	); err != nil {
		t.Fatalf("metrics assert: %v", err)
	}
}
