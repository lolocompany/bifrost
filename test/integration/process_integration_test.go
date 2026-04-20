package integration_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/bifrost"
	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type relayInput struct {
	partition int32
	key       []byte
	value     []byte
}

type relayExpectation struct {
	destPartition   int32
	destKey         []byte
	sourcePartition int32
	sourceOffset    uint64
}

type relayScenario struct {
	name              string
	batchSize         int
	replicas          int
	fromPartitions    int32
	toPartitions      int32
	overridePartition *int32
	overrideKey       *string
	inputs            []relayInput
}

func runRelayScenarios(t *testing.T, brokers []string) {
	t.Helper()

	overridePartition := int32(1)
	overrideKey := "fixed-key"
	scenarios := []relayScenario{
		{
			name:           "batching",
			batchSize:      3,
			replicas:       1,
			fromPartitions: 2,
			toPartitions:   2,
			inputs: []relayInput{
				{partition: 0, key: []byte("k0"), value: []byte("batch-0")},
				{partition: 0, key: []byte("k1"), value: []byte("batch-1")},
				{partition: 0, key: []byte("k2"), value: []byte("batch-2")},
				{partition: 1, key: []byte("k3"), value: []byte("batch-3")},
			},
		},
		{
			name:           "multiple replicas",
			batchSize:      1,
			replicas:       2,
			fromPartitions: 4,
			toPartitions:   4,
			inputs: []relayInput{
				{partition: 0, key: []byte("r0"), value: []byte("replicas-0")},
				{partition: 1, key: []byte("r1"), value: []byte("replicas-1")},
				{partition: 2, key: []byte("r2"), value: []byte("replicas-2")},
				{partition: 3, key: []byte("r3"), value: []byte("replicas-3")},
			},
		},
		{
			name:           "batching with replicas",
			batchSize:      2,
			replicas:       2,
			fromPartitions: 4,
			toPartitions:   4,
			inputs: []relayInput{
				{partition: 0, key: []byte("c0"), value: []byte("combo-0")},
				{partition: 0, key: []byte("c1"), value: []byte("combo-1")},
				{partition: 1, key: []byte("c2"), value: []byte("combo-2")},
				{partition: 1, key: []byte("c3"), value: []byte("combo-3")},
				{partition: 2, key: []byte("c4"), value: []byte("combo-4")},
				{partition: 3, key: []byte("c5"), value: []byte("combo-5")},
			},
		},
		{
			name:              "override partition",
			batchSize:         1,
			replicas:          1,
			fromPartitions:    3,
			toPartitions:      3,
			overridePartition: &overridePartition,
			inputs: []relayInput{
				{partition: 0, key: []byte("p0"), value: []byte("override-partition-0")},
				{partition: 2, key: []byte("p1"), value: []byte("override-partition-1")},
			},
		},
		{
			name:           "override key",
			batchSize:      1,
			replicas:       1,
			fromPartitions: 2,
			toPartitions:   2,
			overrideKey:    &overrideKey,
			inputs: []relayInput{
				{partition: 0, key: []byte("orig-0"), value: []byte("override-key-0")},
				{partition: 1, key: []byte("orig-1"), value: []byte("override-key-1")},
			},
		},
	}

	for _, scenario := range scenarios {
		t.Run(scenario.name, func(t *testing.T) {
			runRelayScenario(t, brokers, scenario)
		})
	}
}

func runRelayScenario(t *testing.T, brokers []string, scenario relayScenario) {
	t.Helper()
	if len(brokers) == 0 {
		t.Fatal("brokers: need at least one seed broker")
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "it.proc.from." + suffix
	toTopic := "it.proc.to." + suffix

	cluster := &bifrostconfig.Cluster{
		Brokers: brokers,
		TLS:     bifrostconfig.TLS{Enabled: false},
		SASL:    bifrostconfig.SASL{Mechanism: "none"},
	}
	pump, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.RecordPartitioner(kgo.ManualPartitioner()),
	)
	if err != nil {
		t.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	mustCreateTopic(t, ctx, pump, fromTopic, scenario.fromPartitions)
	mustCreateTopic(t, ctx, pump, toTopic, scenario.toPartitions)

	expectations := make(map[string]relayExpectation, len(scenario.inputs))
	offsetsByPartition := make(map[int32]uint64)
	for _, input := range scenario.inputs {
		if res := pump.ProduceSync(ctx, &kgo.Record{
			Topic:     fromTopic,
			Partition: input.partition,
			Key:       input.key,
			Value:     input.value,
		}); res.FirstErr() != nil {
			t.Fatalf("seed from-topic: %v", res.FirstErr())
		}

		destPartition := input.partition
		if scenario.overridePartition != nil {
			destPartition = *scenario.overridePartition
		}
		destKey := append([]byte(nil), input.key...)
		if scenario.overrideKey != nil {
			destKey = []byte(*scenario.overrideKey)
		}
		expectations[string(input.value)] = relayExpectation{
			destPartition:   destPartition,
			destKey:         destKey,
			sourcePartition: input.partition,
			sourceOffset:    offsetsByPartition[input.partition],
		}
		offsetsByPartition[input.partition]++
	}

	cfg := bifrostconfig.Config{
		Clusters: map[string]bifrostconfig.Cluster{"it": *cluster},
		Bridges: []bifrostconfig.Bridge{{
			Name:              "itest-process",
			Replicas:          scenario.replicas,
			BatchSize:         scenario.batchSize,
			OverridePartition: scenario.overridePartition,
			OverrideKey:       scenario.overrideKey,
			From:              bifrostconfig.BridgeTarget{Cluster: "it", Topic: fromTopic},
			To:                bifrostconfig.BridgeTarget{Cluster: "it", Topic: toTopic},
		}},
		Metrics: bifrostconfig.Metrics{Enable: boolPtr(false)},
		Logging: bifrostconfig.Logging{
			Level:                 "info",
			Format:                "json",
			Stream:                "stdout",
			PeriodicStatsInterval: "0",
		},
	}

	runCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()
	errCh := make(chan error, 1)
	go func() {
		errCh <- bifrost.Run(runCtx, cfg)
	}()

	verify, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(toTopic),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
		kgo.FetchMaxWait(2*time.Second),
	)
	if err != nil {
		cancel()
		<-errCh
		t.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	seen := make(map[string]struct{}, len(expectations))
	deadline := time.Now().Add(40 * time.Second)
	for len(seen) < len(expectations) && time.Now().Before(deadline) {
		fetches := verify.PollFetches(runCtx)
		if err := fetches.Err(); err != nil {
			if runCtx.Err() != nil {
				break
			}
			t.Fatalf("verify poll: %v", err)
		}
		for _, r := range fetches.Records() {
			if r.Topic != toTopic {
				continue
			}
			valueKey := string(r.Value)
			expectation, ok := expectations[valueKey]
			if !ok {
				continue
			}
			validateRelayedRecord(t, r, expectation, fromTopic)
			seen[valueKey] = struct{}{}
		}
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("bifrost.Run: %v", err)
	}

	if len(seen) != len(expectations) {
		t.Fatalf("relayed records = %d, want %d", len(seen), len(expectations))
	}
}

func mustCreateTopic(t *testing.T, ctx context.Context, cl *kgo.Client, topic string, partitions int32) {
	t.Helper()
	adm := kadm.NewClient(cl)
	resp, err := adm.CreateTopics(ctx, partitions, 1, nil, topic)
	if err != nil {
		t.Fatalf("create topic %q: %v", topic, err)
	}
	for _, result := range resp.Sorted() {
		if result.Err != nil && !errors.Is(result.Err, kerr.TopicAlreadyExists) {
			t.Fatalf("create topic %q: %v", topic, result.Err)
		}
	}
}

func validateRelayedRecord(t *testing.T, record *kgo.Record, expectation relayExpectation, fromTopic string) {
	t.Helper()
	if record.Partition != expectation.destPartition {
		t.Fatalf("destination partition: got %d want %d", record.Partition, expectation.destPartition)
	}
	if string(record.Key) != string(expectation.destKey) {
		t.Fatalf("destination key: got %q want %q", record.Key, expectation.destKey)
	}

	headerVal := func(key string) ([]byte, bool) {
		for _, h := range record.Headers {
			if h.Key == key {
				return h.Value, true
			}
		}
		return nil, false
	}

	if v, ok := headerVal(bridge.HeaderSourceCluster); !ok || string(v) != "it" {
		t.Fatalf("header %s: got %q ok=%v", bridge.HeaderSourceCluster, v, ok)
	}
	if v, ok := headerVal(bridge.HeaderSourceTopic); !ok || string(v) != fromTopic {
		t.Fatalf("header %s: got %q ok=%v", bridge.HeaderSourceTopic, v, ok)
	}
	pv, ok := headerVal(bridge.HeaderSourcePartition)
	if !ok || len(pv) != 4 {
		t.Fatalf("header %s: %v ok=%v", bridge.HeaderSourcePartition, pv, ok)
	}
	ov, ok := headerVal(bridge.HeaderSourceOffset)
	if !ok || len(ov) != 8 {
		t.Fatalf("header %s: %v ok=%v", bridge.HeaderSourceOffset, ov, ok)
	}
	if got := binary.BigEndian.Uint32(pv); got != uint32(expectation.sourcePartition) {
		t.Fatalf("source partition: got %d want %d", got, expectation.sourcePartition)
	}
	if got := binary.BigEndian.Uint64(ov); got != expectation.sourceOffset {
		t.Fatalf("source offset: got %d want %d", got, expectation.sourceOffset)
	}
}

func boolPtr(v bool) *bool { return &v }
