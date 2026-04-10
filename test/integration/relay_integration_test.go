package integration_test

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/testcontainers/testcontainers-go"
	"github.com/twmb/franz-go/pkg/kgo"
)

func requireIntegration(t *testing.T) {
	t.Helper()
	if os.Getenv("BIFROST_INTEGRATION") != "1" {
		t.Skip("set BIFROST_INTEGRATION=1 to run Docker integration tests")
	}
	testcontainers.SkipIfProviderIsNotHealthy(t)
}

// runBridgeRelayTest exercises pkg/bridge against the given Kafka bootstrap broker(s).
func runBridgeRelayTest(t *testing.T, brokers []string) {
	t.Helper()
	if len(brokers) == 0 {
		t.Fatal("brokers: need at least one seed broker")
	}

	ctx := context.Background()
	suffix := fmt.Sprintf("%d", time.Now().UnixNano())
	fromTopic := "bifrost.it.from." + suffix
	toTopic := "bifrost.it.to." + suffix
	want := []byte("hello-bifrost-" + suffix)

	env := &bifrostconfig.Cluster{
		Brokers: brokers,
		TLS:     bifrostconfig.TLS{Enabled: false},
		SASL:    bifrostconfig.SASL{Mechanism: "none"},
	}

	pump, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.AllowAutoTopicCreation(),
	)
	if err != nil {
		t.Fatalf("pump client: %v", err)
	}
	defer pump.Close()

	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: toTopic, Value: nil}); res.FirstErr() != nil {
		t.Fatalf("bootstrap to-topic: %v", res.FirstErr())
	}
	if res := pump.ProduceSync(ctx, &kgo.Record{Topic: fromTopic, Value: want}); res.FirstErr() != nil {
		t.Fatalf("seed from-topic: %v", res.FirstErr())
	}

	metricsOff := false
	reg := prometheus.NewRegistry()
	m, _, err := metrics.New(reg, bifrostconfig.Metrics{Enable: &metricsOff}, []bifrostconfig.Bridge{
		{
			Name: "itest",
			From: bifrostconfig.BridgeTarget{Cluster: "it", Topic: fromTopic},
			To:   bifrostconfig.BridgeTarget{Cluster: "it", Topic: toTopic},
		},
	})
	if err != nil {
		t.Fatalf("metrics: %v", err)
	}

	consumer, err := kafka.NewConsumerForBridge(env, "itest-cg-"+suffix, fromTopic, nil)
	if err != nil {
		t.Fatalf("consumer: %v", err)
	}
	defer consumer.Close()

	producer, err := kafka.NewProducer(env, nil)
	if err != nil {
		t.Fatalf("producer: %v", err)
	}
	defer producer.Close()

	id := bridge.IdentityFrom(bifrostconfig.Bridge{
		Name: "itest",
		From: bifrostconfig.BridgeTarget{Cluster: "it", Topic: fromTopic},
		To:   bifrostconfig.BridgeTarget{Cluster: "it", Topic: toTopic},
	})

	bridgeCtx, cancel := context.WithTimeout(ctx, 45*time.Second)
	defer cancel()

	errCh := make(chan error, 1)
	go func() {
		errCh <- bridge.Run(bridgeCtx, id, consumer, producer, m)
	}()

	verify, err := kgo.NewClient(
		kgo.SeedBrokers(brokers...),
		kgo.ConsumeTopics(toTopic),
		kgo.ConsumerGroup("itest-verify-"+suffix),
		kgo.FetchIsolationLevel(kgo.ReadUncommitted()),
	)
	if err != nil {
		cancel()
		<-errCh
		t.Fatalf("verify client: %v", err)
	}
	defer verify.Close()

	deadline := time.Now().Add(40 * time.Second)
	var got []byte
	var gotRecord *kgo.Record
	for time.Now().Before(deadline) {
		fetches := verify.PollFetches(bridgeCtx)
		if err := fetches.Err(); err != nil {
			if bridgeCtx.Err() != nil {
				break
			}
			t.Fatalf("verify poll: %v", err)
		}
		for _, r := range fetches.Records() {
			if r.Topic == toTopic && string(r.Value) == string(want) {
				got = append([]byte(nil), r.Value...)
				cp := *r
				gotRecord = &cp
				break
			}
		}
		if got != nil {
			break
		}
	}

	cancel()
	if err := <-errCh; err != nil && !errors.Is(err, context.Canceled) {
		t.Fatalf("bridge: %v", err)
	}

	if got == nil {
		t.Fatal("timed out waiting for relayed record on to-topic")
	}
	if string(got) != string(want) {
		t.Fatalf("value: got %q want %q", got, want)
	}

	headerVal := func(key string) ([]byte, bool) {
		for _, h := range gotRecord.Headers {
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
	// First payload on from-topic should be partition 0, offset 0 for this test.
	if binary.BigEndian.Uint32(pv) != 0 {
		t.Fatalf("source partition: got %d want 0", binary.BigEndian.Uint32(pv))
	}
	if binary.BigEndian.Uint64(ov) != 0 {
		t.Fatalf("source offset: got %d want 0", binary.BigEndian.Uint64(ov))
	}
}
