package integration_test

import (
	"context"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/internal/domain/relay"
	"github.com/lolocompany/bifrost/test/integration/testutil/artifacts"
	kafkautil "github.com/lolocompany/bifrost/test/integration/testutil/kafka"
	metricutil "github.com/lolocompany/bifrost/test/integration/testutil/metrics"
	processutil "github.com/lolocompany/bifrost/test/integration/testutil/process"
	"github.com/lolocompany/bifrost/test/integration/testutil/scenario"
	"github.com/twmb/franz-go/pkg/kgo"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	kafkautil.RequireIntegration(t)
}

func runBridgeRelayTest(t *testing.T, provider kafkautil.Provider) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	brokers := kafkautil.StartCluster(t, ctx, provider)
	pump := kafkautil.NewClient(t, brokers)
	defer pump.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "app.it.from." + suffix
	toTopic := "app.it.to." + suffix
	want := []byte("hello-bifrost-" + suffix)

	kafkautil.MustCreateTopic(t, ctx, pump, fromTopic, 1)
	kafkautil.MustCreateTopic(t, ctx, pump, toTopic, 1)
	kafkautil.ProduceSync(t, ctx, pump, &kgo.Record{Topic: fromTopic, Value: want})

	metricsAddr, err := processutil.FreeTCPAddr()
	if err != nil {
		t.Fatalf("metrics addr: %v", err)
	}

	tmpl, err := processutil.ReadFile(filepath.Join("fixtures", "single_bridge.yaml.tmpl"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	rendered, err := scenario.RenderConfig(string(tmpl), scenario.ConfigData{
		FromTopic:           fromTopic,
		ToTopic:             toTopic,
		FromCluster:         "a",
		ToCluster:           "b",
		BrokersFrom:         brokers,
		BrokersTo:           brokers,
		MetricsAddr:         metricsAddr,
		BridgeName:          "itest",
		ConsumerGroup:       "itest-cg-" + suffix,
		BatchSize:           1,
		Replicas:            1,
		HasOverrideKey:      false,
		HeadersSourceFormat: "verbose",
	})
	if err != nil {
		t.Fatalf("render config: %v", err)
	}

	art := artifacts.New(t, "integration")
	if err := art.WriteConfig(rendered); err != nil {
		t.Fatalf("write config: %v", err)
	}

	proc, err := processutil.Start(ctx, art, metricsAddr)
	if err != nil {
		t.Fatalf("start bifrost: %v", err)
	}
	t.Cleanup(func() {
		if err := proc.Stop(); err != nil {
			t.Errorf("stop bifrost: %v", err)
		}
	})

	readyCtx, readyCancel := context.WithTimeout(ctx, 20*time.Second)
	defer readyCancel()
	if err := proc.WaitReady(readyCtx); err != nil {
		t.Fatalf("wait ready: %v", err)
	}

	verify := kafkautil.NewClient(t, brokers,
		kgo.ConsumeTopics(toTopic),
		kgo.ConsumerGroup("itest-verify-"+suffix),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	)
	defer verify.Close()

	records, err := kafkautil.WaitForRecords(ctx, verify, toTopic, 1, func(r *kgo.Record) bool {
		return string(r.Value) == string(want)
	})
	if err != nil {
		t.Fatalf("wait records: %v", err)
	}
	gotRecord := records[0]
	if string(gotRecord.Value) != string(want) {
		t.Fatalf("value: got %q want %q", gotRecord.Value, want)
	}

	headerVal := func(key string) ([]byte, bool) {
		for _, h := range gotRecord.Headers {
			if h.Key == key {
				return h.Value, true
			}
		}
		return nil, false
	}
	if v, ok := headerVal(relay.HeaderCourseHash); !ok || len(v) != 32 {
		t.Fatalf("header %s: len=%d ok=%v", relay.HeaderCourseHash, len(v), ok)
	}
	if v, ok := headerVal(relay.HeaderSourceCluster); !ok || string(v) != "a" {
		t.Fatalf("header %s: got %q ok=%v", relay.HeaderSourceCluster, v, ok)
	}
	if v, ok := headerVal(relay.HeaderSourceTopic); !ok || string(v) != fromTopic {
		t.Fatalf("header %s: got %q ok=%v", relay.HeaderSourceTopic, v, ok)
	}
	pv, ok := headerVal(relay.HeaderSourcePartition)
	if !ok || len(pv) != 4 || binary.BigEndian.Uint32(pv) != 0 {
		t.Fatalf("source partition header invalid: %v ok=%v", pv, ok)
	}
	ov, ok := headerVal(relay.HeaderSourceOffset)
	if !ok || len(ov) != 8 || binary.BigEndian.Uint64(ov) != 0 {
		t.Fatalf("source offset header invalid: %v ok=%v", ov, ok)
	}

	metricsBody, err := metricutil.WaitContains("http://"+metricsAddr+"/metrics", 10*time.Second,
		"bifrost_relay_messages_total",
		"bifrost_relay_produce_duration_seconds",
	)
	if err != nil {
		t.Fatalf("metrics assert: %v", err)
	}
	if len(metricsBody) == 0 {
		t.Fatal("metrics body empty")
	}
}
