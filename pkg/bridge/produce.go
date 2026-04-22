package bridge

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func mapBatchToOutput(id Identity, batch []*kgo.Record, opts RunOptions) []*kgo.Record {
	out := make([]*kgo.Record, 0, len(batch))
	for _, r := range batch {
		headers := make([]kgo.RecordHeader, 0, len(r.Headers)+4+len(opts.ExtraHeaders))
		headers = AppendSourceHeaders(headers, id, r)
		headers = append(headers, opts.ExtraHeaders...)
		headers = append(headers, r.Headers...)
		record := &kgo.Record{
			Topic:     id.ToTopic,
			Key:       r.Key,
			Value:     r.Value,
			Headers:   headers,
			Timestamp: r.Timestamp,
		}
		if opts.OverridePartition != nil {
			record.Partition = *opts.OverridePartition
		} else {
			record.Partition = r.Partition
		}
		if opts.OverrideKey != nil {
			record.Key = append([]byte(nil), opts.OverrideKey...)
		}
		out = append(out, record)
	}
	return out
}

func produceBatchAsync(ctx context.Context, producer ProducerClient, batch []*kgo.Record) error {
	if len(batch) == 0 {
		return nil
	}
	type syncBatchProducer interface {
		ProduceSyncBatch(context.Context, []*kgo.Record) error
	}
	if bp, ok := producer.(syncBatchProducer); ok {
		return bp.ProduceSyncBatch(ctx, batch)
	}
	errCh := make(chan error, len(batch))
	for _, record := range batch {
		producer.Produce(ctx, record, func(_ *kgo.Record, err error) {
			errCh <- err
		})
	}
	var first error
	for range batch {
		select {
		case err := <-errCh:
			if err != nil && first == nil {
				first = err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return first
}
