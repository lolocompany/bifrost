package bridge_test

import (
	"context"
	"errors"
	"strings"
	"sync"
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{}))
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		ExtraHeaders: []kgo.RecordHeader{
			{Key: "env", Value: []byte("prod")},
		},
	}))
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

	err := bridge.RunWithClients(context.Background(), testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{}))
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{MinBackoff: time.Second, MaxBackoff: time.Second},
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	}))
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		Retry: bridge.RetryPolicy{
			Commit: bridge.RetryConfig{MinBackoff: 2 * time.Second, MaxBackoff: 2 * time.Second},
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	}))
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		BatchSize: 2,
	}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if producer.produceCalls != 3 {
		t.Fatalf("produce calls = %d, want 3", producer.produceCalls)
	}
	if consumer.commitCalls != 2 {
		t.Fatalf("commit calls = %d, want 2", consumer.commitCalls)
	}
	if got := len(consumer.commitBatches[0]); got != 1 {
		t.Fatalf("first commit batch size = %d, want 1", got)
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		OverridePartition: &overridePartition,
		OverrideKey:       []byte("override-key"),
	}))
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		BatchSize: 2,
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{MinBackoff: time.Second, MaxBackoff: time.Second},
		},
		Sleep: func(_ context.Context, d time.Duration) error {
			sleeps = append(sleeps, d)
			return nil
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if producer.produceCalls != 4 {
		t.Fatalf("produce calls = %d, want 4", producer.produceCalls)
	}
	if consumer.commitCalls != 1 {
		t.Fatalf("commit calls = %d, want 1", consumer.commitCalls)
	}
	if got := len(consumer.commitBatches[0]); got != 1 {
		t.Fatalf("commit batch size = %d, want 1", got)
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{}))
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

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{
		Retry: bridge.RetryPolicy{
			Produce: bridge.RetryConfig{MinBackoff: time.Second, MaxBackoff: time.Second},
		},
		Sleep: func(context.Context, time.Duration) error {
			sleepCalls++
			cancel()
			return context.Canceled
		},
		Jitter: func(time.Duration) time.Duration { return 0 },
	}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if sleepCalls != 1 {
		t.Fatalf("sleep calls = %d, want 1", sleepCalls)
	}
}

func TestRunWithClients_commitCoalescingByThreshold(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{
				{Topic: "from-topic", Partition: 0, Offset: 1, Value: []byte("a")},
				{Topic: "from-topic", Partition: 0, Offset: 2, Value: []byte("b")},
			},
		}},
		commitHook: cancel,
	}
	producer := &fakeProducer{}
	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		BatchSize:          1,
		MaxInFlightBatches: 1,
		CommitMaxRecords:   2,
		CommitInterval:     time.Second,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if got := consumer.commitCalls; got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}
	if got := len(consumer.commitBatches[0]); got != 1 {
		t.Fatalf("commit batch size = %d, want 1 partition head", got)
	}
	if got := m.messages; got != 2 {
		t.Fatalf("messages = %d, want 2", got)
	}
}

func TestRunWithClients_zeroCommitIntervalDoesNotPanic(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		polls: []fakeFetches{{
			records: []*kgo.Record{{Topic: "from-topic", Partition: 0, Offset: 1, Value: []byte("x")}},
		}},
		commitHook: cancel,
	}
	producer := &fakeProducer{}
	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, bridge.RunOptions{
		BatchSize:          1,
		MaxInFlightBatches: 1,
		CommitInterval:     0,
		CommitMaxRecords:   1,
	})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if got := consumer.commitCalls; got != 1 {
		t.Fatalf("commit calls = %d, want 1", got)
	}
}

func TestRunWithClients_attributesConsumerAndProducerBusyIdleSeconds(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancel(context.Background())
	m := &testMetricsReporter{}
	consumer := &fakeConsumer{
		pollDelay: 2 * time.Millisecond,
		polls: []fakeFetches{
			{},
			{records: []*kgo.Record{{Topic: "from-topic", Value: []byte("payload")}}},
		},
		commitHook: cancel,
	}
	producer := &fakeProducer{produceDelay: 3 * time.Millisecond}

	err := bridge.RunWithClients(ctx, testIdentity(), consumer, producer, m, testRunOptions(bridge.RunOptions{}))
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("RunWithClients error = %v, want context canceled", err)
	}
	if got := m.consumerSeconds["idle"]; got <= 0 {
		t.Fatalf("consumer idle seconds = %f, want > 0", got)
	}
	if got := m.consumerSeconds["busy"]; got <= 0 {
		t.Fatalf("consumer busy seconds = %f, want > 0", got)
	}
	if got := m.producerSeconds["idle"]; got <= 0 {
		t.Fatalf("producer idle seconds = %f, want > 0", got)
	}
	if got := m.producerSeconds["busy"]; got <= 0 {
		t.Fatalf("producer busy seconds = %f, want > 0", got)
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
	pollDelay     time.Duration
}

func (f *fakeConsumer) PollFetches(context.Context) bridge.FetchResult {
	f.pollCalls++
	if f.pollDelay > 0 {
		time.Sleep(f.pollDelay)
	}
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
	produceDelay    time.Duration
}

func (f *fakeProducer) Produce(_ context.Context, r *kgo.Record, promise func(*kgo.Record, error)) {
	var err error
	f.produceCalls++
	if f.produceDelay > 0 {
		time.Sleep(f.produceDelay)
	}
	batch := []*kgo.Record{r}
	f.lastBatch = batch
	f.producedBatches = append(f.producedBatches, batch)
	if len(f.results) > 0 {
		err = f.results[0]
		f.results = f.results[1:]
	} else {
		err = f.err
	}
	promise(r, err)
}

type fakeFetches struct {
	err     error
	records []*kgo.Record
}

func (f fakeFetches) Err() error             { return f.err }
func (f fakeFetches) NumRecords() int        { return len(f.records) }
func (f fakeFetches) Records() []*kgo.Record { return f.records }

type testMetricsReporter struct {
	mu               sync.Mutex
	messages         int
	errors           map[string]int
	produceDurations []float64
	consumerSeconds  map[string]float64
	producerSeconds  map[string]float64
}

func (m *testMetricsReporter) IncMessages(bridge.Identity) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages++
}

func (m *testMetricsReporter) IncErrors(_ bridge.Identity, stage string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.errors == nil {
		m.errors = make(map[string]int)
	}
	m.errors[stage]++
}

func (m *testMetricsReporter) ObserveProduceDuration(_ bridge.Identity, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.produceDurations = append(m.produceDurations, seconds)
}

func (m *testMetricsReporter) AddConsumerSeconds(_ bridge.Identity, state string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.consumerSeconds == nil {
		m.consumerSeconds = make(map[string]float64)
	}
	m.consumerSeconds[state] += seconds
}

func (m *testMetricsReporter) AddProducerSeconds(_ bridge.Identity, state string, seconds float64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.producerSeconds == nil {
		m.producerSeconds = make(map[string]float64)
	}
	m.producerSeconds[state] += seconds
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

func testRunOptions(opts bridge.RunOptions) bridge.RunOptions {
	if opts.MaxInFlightBatches == 0 {
		opts.MaxInFlightBatches = 1
	}
	if opts.CommitMaxRecords == 0 {
		opts.CommitMaxRecords = 1
	}
	if opts.CommitInterval == 0 {
		opts.CommitInterval = time.Millisecond
	}
	return opts
}
