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

type relayInput struct {
	partition int32
	key       []byte
	value     []byte
}

type relayScenario struct {
	name         string
	batchSize    int
	replicas     int
	fromParts    int32
	toParts      int32
	overrideKey  string
	inputs       []relayInput
	extraHeaders map[string]string
}

func runRelayScenarios(t *testing.T, provider kafkautil.Provider) {
	t.Helper()
	scenarios := []relayScenario{
		{
			name:      "batching",
			batchSize: 3, replicas: 1, fromParts: 2, toParts: 2,
			inputs: []relayInput{
				{partition: 0, key: []byte("k0"), value: []byte("batch-0")},
				{partition: 0, key: []byte("k1"), value: []byte("batch-1")},
				{partition: 1, key: []byte("k2"), value: []byte("batch-2")},
			},
		},
		{
			name:      "multiple replicas",
			batchSize: 1, replicas: 2, fromParts: 4, toParts: 4,
			inputs: []relayInput{
				{partition: 0, key: []byte("r0"), value: []byte("replicas-0")},
				{partition: 1, key: []byte("r1"), value: []byte("replicas-1")},
				{partition: 2, key: []byte("r2"), value: []byte("replicas-2")},
			},
		},
		{
			name:        "override key",
			batchSize:   1,
			replicas:    1,
			fromParts:   2,
			toParts:     2,
			overrideKey: "fixed-key",
			inputs: []relayInput{
				{partition: 0, key: []byte("orig-0"), value: []byte("override-key-0")},
				{partition: 1, key: []byte("orig-1"), value: []byte("override-key-1")},
			},
		},
		{
			name:      "extra headers",
			batchSize: 1, replicas: 1, fromParts: 2, toParts: 2,
			extraHeaders: map[string]string{
				"env": "itest",
			},
			inputs: []relayInput{
				{partition: 0, key: []byte("h0"), value: []byte("extra-h-0")},
				{partition: 1, key: []byte("h1"), value: []byte("extra-h-1")},
			},
		},
	}
	for _, sc := range scenarios {
		t.Run(sc.name, func(t *testing.T) {
			runRelayScenario(t, provider, sc)
		})
	}
}

func runRelayScenario(t *testing.T, provider kafkautil.Provider, sc relayScenario) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()
	brokers := kafkautil.StartCluster(t, ctx, provider)
	pump := kafkautil.NewClient(t, brokers, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	defer pump.Close()

	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "it.proc.from." + suffix
	toTopic := "it.proc.to." + suffix
	kafkautil.MustCreateTopic(t, ctx, pump, fromTopic, sc.fromParts)
	kafkautil.MustCreateTopic(t, ctx, pump, toTopic, sc.toParts)
	expected := make(map[string]relayInput, len(sc.inputs))
	offsetByPart := map[int32]uint64{}
	for _, in := range sc.inputs {
		kafkautil.ProduceSync(t, ctx, pump, &kgo.Record{
			Topic: fromTopic, Partition: in.partition, Key: in.key, Value: in.value,
		})
		expected[string(in.value)] = in
		offsetByPart[in.partition]++
	}

	metricsAddr, err := processutil.FreeTCPAddr()
	if err != nil {
		t.Fatalf("metrics addr: %v", err)
	}
	tmpl, err := processutil.ReadFile(filepath.Join("fixtures", "single_bridge.yaml.tmpl"))
	if err != nil {
		t.Fatalf("read fixture: %v", err)
	}
	data := scenario.ConfigData{
		FromTopic:           fromTopic,
		ToTopic:             toTopic,
		FromCluster:         "a",
		ToCluster:           "b",
		BrokersFrom:         brokers,
		BrokersTo:           brokers,
		MetricsAddr:         metricsAddr,
		BridgeName:          "itest-process",
		ConsumerGroup:       "itest-cg-" + suffix,
		BatchSize:           sc.batchSize,
		Replicas:            sc.replicas,
		HasOverrideKey:      sc.overrideKey != "",
		OverrideKey:         sc.overrideKey,
		ExtraHeaders:        sc.extraHeaders,
		HeadersSourceFormat: "verbose",
	}
	cfg, err := scenario.RenderConfig(string(tmpl), data)
	if err != nil {
		t.Fatalf("render config: %v", err)
	}
	art := artifacts.New(t, "integration")
	if err := art.WriteConfig(cfg); err != nil {
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
	if err := proc.WaitReady(ctx); err != nil {
		t.Fatalf("wait ready: %v", err)
	}

	verify := kafkautil.NewClient(t, brokers,
		kgo.ConsumeTopics(toTopic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	)
	defer verify.Close()
	records, err := kafkautil.WaitForRecords(ctx, verify, toTopic, len(sc.inputs), nil)
	if err != nil {
		t.Fatalf("wait records: %v", err)
	}
	for _, r := range records {
		in, ok := expected[string(r.Value)]
		if !ok {
			continue
		}
		if sc.overrideKey != "" {
			if string(r.Key) != sc.overrideKey {
				t.Fatalf("override key: got %q want %q", r.Key, sc.overrideKey)
			}
		} else if string(r.Key) != string(in.key) {
			t.Fatalf("key mismatch: got %q want %q", r.Key, in.key)
		}
		hdr := headerValue(r.Headers, relay.HeaderSourcePartition)
		if len(hdr) != 4 {
			t.Fatalf("source partition header missing")
		}
		if got := binary.BigEndian.Uint32(hdr); got != uint32(in.partition) {
			t.Fatalf("source partition=%d want=%d", got, in.partition)
		}
		for k, v := range sc.extraHeaders {
			got := headerValue(r.Headers, k)
			if string(got) != v {
				t.Fatalf("extra header %q got %q want %q", k, got, v)
			}
		}
	}
	if _, err := metricutil.WaitContains("http://"+metricsAddr+"/metrics", 10*time.Second,
		"bifrost_relay_messages_total",
		`stage="poll"`,
	); err != nil {
		t.Fatalf("metrics assert: %v", err)
	}
}

func headerValue(headers []kgo.RecordHeader, key string) []byte {
	for _, h := range headers {
		if h.Key == key {
			return h.Value
		}
	}
	return nil
}
