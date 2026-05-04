package relay

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

func mapBatchToOutput(id Identity, batch []*kgo.Record, opts Options) []*kgo.Record {
	out := make([]*kgo.Record, 0, len(batch))
	srcOn := opts.EffectiveSourceHeadersEnabled()
	propagate := opts.EffectivePropagateRecordHeaders()
	for _, r := range batch {
		part, off := NormalizedSourceCoord(r)
		nSrc := 0
		if srcOn {
			nSrc = 1
			if opts.SourceHeadersVerbose {
				nSrc += 4
			}
		}
		nExtra := len(opts.ExtraHeaders)
		nProp := 0
		if propagate {
			nProp = len(r.Headers)
		}
		headers := make([]kgo.RecordHeader, 0, nSrc+nExtra+nProp)

		if srcOn {
			headers = AppendCourseHashHeader(headers, id, r.Topic, part, off)
			if opts.SourceHeadersVerbose {
				headers = AppendSourceHeaders(headers, id, r.Topic, part, off)
			}
		}
		headers = append(headers, opts.ExtraHeaders...)
		if propagate {
			headers = append(headers, r.Headers...)
		}
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
	var firstErr error
	for range batch {
		select {
		case err := <-errCh:
			if err != nil && firstErr == nil {
				firstErr = err
			}
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	return firstErr
}
