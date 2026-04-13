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

func (f *fakeConsumer) CommitRecords(context.Context, ...*kgo.Record) error {
	f.commitCalls++
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
	err          error
	results      []error
	produceCalls int
}

func (f *fakeProducer) ProduceSync(context.Context, *kgo.Record) bridge.ProduceResult {
	f.produceCalls++
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
