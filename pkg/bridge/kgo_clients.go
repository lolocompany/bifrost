package bridge

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

type kgoConsumer struct {
	client *kgo.Client
}

func (c kgoConsumer) PollFetches(ctx context.Context) FetchResult {
	return kgoFetches{fetches: c.client.PollFetches(ctx)}
}

func (c kgoConsumer) CommitRecords(ctx context.Context, records ...*kgo.Record) error {
	return c.client.CommitRecords(ctx, records...)
}

type kgoProducer struct {
	client *kgo.Client
}

func (p kgoProducer) Produce(ctx context.Context, record *kgo.Record, promise func(*kgo.Record, error)) {
	p.client.Produce(ctx, record, promise)
}

func (p kgoProducer) ProduceSyncBatch(ctx context.Context, batch []*kgo.Record) error {
	return p.client.ProduceSync(ctx, batch...).FirstErr()
}

type kgoFetches struct {
	fetches kgo.Fetches
}

func (f kgoFetches) Err() error             { return f.fetches.Err() }
func (f kgoFetches) NumRecords() int        { return f.fetches.NumRecords() }
func (f kgoFetches) Records() []*kgo.Record { return f.fetches.Records() }
