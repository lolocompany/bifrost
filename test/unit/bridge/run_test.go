package bridge_test

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestRunWithClients_retriesAfterPollError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	wantErr := errors.New("broker unavailable")
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{
			{err: wantErr},
			{records: []*kgo.Record{{Topic: "from-topic", Value: []byte("payload")}}},
		},
		commitHook: func() {
			cancel()
		},
	}
	producer := &fakeProducer{}

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if got := m.errors["poll"]; got != 1 {
		t.Fatalf("poll error count = %d, want 1", got)
	}
	if got := m.messages; got != 1 {
		t.Fatalf("messages count = %d, want 1", got)
	}
	if producer.produceCalls != 1 {
		t.Fatalf("produce calls = %d, want 1", producer.produceCalls)
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", consumer.commitCalls)
	}
}

func TestRunWithClients_includesExtraHeadersAfterSourceHeaders(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{
				Topic:   "from-topic",
				Value:   []byte("payload"),
				Headers: []kgo.RecordHeader{{Key: "upstream", Value: []byte("1")}},
			}},
		}},
		commitHook: func() { cancel() },
	}
	producer := &fakeProducer{}

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		ExtraHeaders: []kgo.RecordHeader{
			{Key: "env", Value: []byte("prod")},
		},
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if len(producer.lastBatch) != 1 || producer.lastBatch[0] == nil {
		t.Fatal("expected produced record")
	}
	h := producer.lastBatch[0].Headers
	if len(h) != 6 {
		t.Fatalf("header count = %d, want 6 (4 source + 1 extra + 1 upstream)", len(h))
	}
	if string(h[4].Key) != "env" || string(h[4].Value) != "prod" {
		t.Fatalf("extra header: %+v", h[4])
	}
	if string(h[5].Key) != "upstream" {
		t.Fatalf("upstream header position: %+v", h[5])
	}
}

func TestRunWithClients_returnsWrongTopicError(t *testing.T) {
	t.Parallel()
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{Topic: "wrong-topic"}},
		}},
	}
	producer := &fakeProducer{}

	err := bridge.RunWithClients(context.Background(), testIdentity(), consumer, producer, m, bridge.RunOptions{})
	if err == nil || !strings.Contains(err.Error(), "unexpected topic") {
		t.Fatalf("RunWithClients error = %v, want unexpected topic error", err)
	}
	if got := m.errors["route"]; got != 1 {
		t.Fatalf("route error count = %d, want 1", got)
	}
	if producer.produceCalls != 0 {
		t.Fatalf("produce calls = %d, want 0", producer.produceCalls)
	}
}

func TestRunWithClients_retriesProduceErrorWithBackoff(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{Topic: "from-topic", Value: []byte("payload")}},
		}},
		commitHook: func() {
			cancel()
		},
	}
	producer := &fakeProducer{results: []error{errors.New("produce failed"), nil}}
	var sleeps []time.Duration

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{MinBackoff: time.Second, MaxBackoff: time.Second},
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if got := m.errors["produce"]; got != 1 {
		t.Fatalf("produce error count = %d, want 1", got)
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", consumer.commitCalls)
	}
	if producer.produceCalls != 2 {
		t.Fatalf("produce calls = %d, want 2", producer.produceCalls)
	}
	if len(sleeps) != 1 || sleeps[0] != time.Second {
		t.Fatalf("sleeps = %v, want [1s]", sleeps)
	}
}

func TestRunWithClients_retriesCommitErrorInPlace(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{Topic: "from-topic", Value: []byte("payload")}},
		}},
		commitResults: []error{errors.New("commit failed"), nil},
	}
	consumer.commitHook = func() {
		if consumer.commitCalls == 2 {
			cancel()
		}
	}
	producer := &fakeProducer{}
	var sleeps []time.Duration

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		Retry: bridge.RetryPolicy{
			Commit: bridge.RetryConfig{MinBackoff: 2 * time.Second, MaxBackoff: 2 * time.Second},
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if got := m.errors["commit"]; got != 1 {
		t.Fatalf("commit error count = %d, want 1", got)
	}
	if got := len(m.produceDurations); got != 1 {
		t.Fatalf("produce duration observations = %d, want 1", got)
	}
	if got := m.messages; got != 1 {
		t.Fatalf("messages count = %d, want 1", got)
	}
	if consumer.commitCalls != 2 {
		t.Fatalf("commit calls = %d, want 2", consumer.commitCalls)
	}
	if producer.produceCalls != 1 {
		t.Fatalf("produce calls = %d, want 1", producer.produceCalls)
	}
	if len(sleeps) != 1 || sleeps[0] != 2*time.Second {
		t.Fatalf("sleeps = %v, want [2s]", sleeps)
	}
}

func TestRunWithClients_batchesByPartitionAndPreservesSourcePartition(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	var consumer *fakeConsumer
	consumer = &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{
				{Topic: "from-topic", Partition: 4, Offset: 10, Value: []byte("a")},
				{Topic: "from-topic", Partition: 4, Offset: 11, Value: []byte("b")},
				{Topic: "from-topic", Partition: 7, Offset: 20, Value: []byte("c")},
			},
		}},
		commitHook: func() {
			if consumer.commitCalls == 2 {
				cancel()
			}
		},
	}
	producer := &fakeProducer{}

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		BatchSize: 2,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if producer.produceCalls != 2 {
		t.Fatalf("produce calls = %d, want 2", producer.produceCalls)
	}
	if len(producer.producedBatches) != 2 {
		t.Fatalf("produced batches = %d, want 2", len(producer.producedBatches))
	}
	if len(producer.producedBatches[0]) != 2 {
		t.Fatalf("first batch size = %d, want 2", len(producer.producedBatches[0]))
	}
	for i, r := range producer.producedBatches[0] {
		if r.Partition != 4 {
			t.Fatalf("first batch record %d partition = %d, want 4", i, r.Partition)
		}
	}
	if len(producer.producedBatches[1]) != 1 {
		t.Fatalf("second batch size = %d, want 1", len(producer.producedBatches[1]))
	}
	if got := producer.producedBatches[1][0].Partition; got != 7 {
		t.Fatalf("second batch partition = %d, want 7", got)
	}
	if consumer.commitCalls != 2 {
		t.Fatalf("commit calls = %d, want 2", consumer.commitCalls)
	}
	if got := len(consumer.commitBatches[0]); got != 2 {
		t.Fatalf("first commit batch size = %d, want 2", got)
	}
	if got := len(consumer.commitBatches[1]); got != 1 {
		t.Fatalf("second commit batch size = %d, want 1", got)
	}
	if got := m.messages; got != 3 {
		t.Fatalf("messages count = %d, want 3", got)
	}
}

func TestRunWithClients_canOverridePartitionAndKey(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{Topic: "from-topic", Partition: 4, Offset: 10, Value: []byte("payload")}},
		}},
		commitHook: func() { cancel() },
	}
	producer := &fakeProducer{}
	overridePartition := int32(2)

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		OverridePartition: &overridePartition,
		OverrideKey:       []byte("override-key"),
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if len(producer.lastBatch) != 1 {
		t.Fatalf("last batch size = %d, want 1", len(producer.lastBatch))
	}
	if got := producer.lastBatch[0].Partition; got != 2 {
		t.Fatalf("produced partition = %d, want 2 when overridden", got)
	}
	if got := string(producer.lastBatch[0].Key); got != "override-key" {
		t.Fatalf("produced key = %q, want override-key", got)
	}
}

func TestRunWithClients_retriesWholeBatchOnProduceError(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{
				{Topic: "from-topic", Partition: 4, Offset: 10, Value: []byte("a")},
				{Topic: "from-topic", Partition: 4, Offset: 11, Value: []byte("b")},
			},
		}},
		commitHook: func() { cancel() },
	}
	producer := &fakeProducer{results: []error{errors.New("produce failed"), nil}}
	var sleeps []time.Duration

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		BatchSize: 2,
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{MinBackoff: time.Second, MaxBackoff: time.Second},
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if producer.produceCalls != 2 {
		t.Fatalf("produce calls = %d, want 2", producer.produceCalls)
	}
	if len(producer.producedBatches) != 2 {
		t.Fatalf("produced batches = %d, want 2", len(producer.producedBatches))
	}
	for attempt, batch := range producer.producedBatches {
		if len(batch) != 2 {
			t.Fatalf("attempt %d batch size = %d, want 2", attempt, len(batch))
		}
		if string(batch[0].Value) != "a" || string(batch[1].Value) != "b" {
			t.Fatalf("attempt %d batch values = [%s %s], want [a b]", attempt, batch[0].Value, batch[1].Value)
		}
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", consumer.commitCalls)
	}
	if got := len(consumer.commitBatches[0]); got != 2 {
		t.Fatalf("commit batch size = %d, want 2", got)
	}
	if len(sleeps) != 1 || sleeps[0] != time.Second {
		t.Fatalf("sleeps = %v, want [1s]", sleeps)
	}
	if got := m.errors["produce"]; got != 1 {
		t.Fatalf("produce error count = %d, want 1", got)
	}
	if got := m.messages; got != 2 {
		t.Fatalf("messages count = %d, want 2", got)
	}
}

func TestRunWithClients_returnsContextCanceledBeforePolling(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	m := &testMetricsReporter{}
	consumer := &fakeConsumer{}
	producer := &fakeProducer{}

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if consumer.pollCalls != 0 {
		t.Fatalf("poll calls = %d, want 0", consumer.pollCalls)
	}
}

func TestRunWithClients_stopsRetrySleepOnContextCancellation(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{Topic: "from-topic", Value: []byte("payload")}},
		}},
	}
	producer := &fakeProducer{results: []error{errors.New("produce failed")}}
	sleepCalls := 0

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{MinBackoff: time.Second, MaxBackoff: time.Second},
		},
		Sleep: func(context.Context, time.Duration) error {
			sleepCalls++
			cancel()
			return context.Canceled
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if sleepCalls != 1 {
		t.Fatalf("sleep calls = %d, want 1", sleepCalls)
	}
}

type fakeConsumer struct {
	polls         []fakeFetches
	commitErr     error
	commitResults []error
	commitHook    func()
	commitBatches [][]*kgo.Record
	pollCalls     int
	commitCalls   int
}

func (f *fakeConsumer) PollFetches(context.Context) bridge.FetchResult {
	f.pollCalls++
	if len(f.polls) == 0 {
		return fakeFetches{}
	}
	next := f.polls[0]
	f.polls = f.polls[1:]
	return next
}

func (f *fakeConsumer) CommitRecords(_ context.Context, records ...*kgo.Record) error {
	f.commitCalls++
	batch := append([]*kgo.Record(nil), records...)
	f.commitBatches = append(f.commitBatches, batch)
	if f.commitHook != nil {
		defer f.commitHook()
	}
	if len(f.commitResults) > 0 {
		next := f.commitResults[0]
		f.commitResults = f.commitResults[1:]
		return next
	}
	return f.commitErr
}

type fakeProducer struct {
	err             error
	results         []error
	produceCalls    int
	lastBatch       []*kgo.Record
	producedBatches [][]*kgo.Record
}

func (f *fakeProducer) ProduceSync(_ context.Context, rs ...*kgo.Record) bridge.ProduceResult {
	f.produceCalls++
	batch := append([]*kgo.Record(nil), rs...)
	f.lastBatch = batch
	f.producedBatches = append(f.producedBatches, batch)
	if len(f.results) > 0 {
		next := f.results[0]
		f.results = f.results[1:]
		return fakeProduceResult{err: next}
	}
	return fakeProduceResult{err: f.err}
}

type fakeFetches struct {
	err     error
	records []*kgo.Record
}

func (f fakeFetches) Err() error             { return f.err }
func (f fakeFetches) NumRecords() int        { return len(f.records) }
func (f fakeFetches) Records() []*kgo.Record { return f.records }

type fakeProduceResult struct {
	err error
}

func (f fakeProduceResult) FirstErr() error { return f.err }

type testMetricsReporter struct {
	messages         int
	errors           map[string]int
	produceDurations []float64
}

func (m *testMetricsReporter) IncMessages(bridge.Identity) {
	m.messages++
}

func (m *testMetricsReporter) IncErrors(_ bridge.Identity, stage string) {
	if m.errors == nil {
		m.errors = make(map[string]int)
	}
	m.errors[stage]++
}

func (m *testMetricsReporter) ObserveProduceDuration(_ bridge.Identity, seconds float64) {
	m.produceDurations = append(m.produceDurations, seconds)
}

func testIdentity() bridge.Identity {
	return bridge.Identity{
		BridgeName:  "bridge-a",
		FromCluster: "cluster-a",
		FromTopic:   "from-topic",
		ToCluster:   "cluster-b",
		ToTopic:     "to-topic",
	}
}
