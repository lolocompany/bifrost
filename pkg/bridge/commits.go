package bridge

import (
	"context"
	"fmt"
	"log/slog"
	"sync/atomic"

	"github.com/twmb/franz-go/pkg/kgo"
)

type commitCandidate struct {
	partition int32
	records   []*kgo.Record
}

type commitAggregator struct {
	heads   map[int32]*kgo.Record
	pending int
}

func newCommitAggregator(_ int) *commitAggregator {
	return &commitAggregator{heads: make(map[int32]*kgo.Record)}
}

func (c *commitAggregator) add(partition int32, records ...*kgo.Record) {
	if len(records) == 0 {
		return
	}
	c.heads[partition] = records[len(records)-1]
	c.pending += len(records)
}

func (c *commitAggregator) pendingRecords() int {
	return c.pending
}

func (c *commitAggregator) drain() []*kgo.Record {
	if len(c.heads) == 0 {
		return nil
	}
	out := make([]*kgo.Record, 0, len(c.heads))
	for _, r := range c.heads {
		out = append(out, r)
	}
	c.heads = make(map[int32]*kgo.Record)
	c.pending = 0
	return out
}

func flushCommits(
	ctx context.Context,
	log *slog.Logger,
	consumer ConsumerClient,
	errorsSeen *atomic.Uint64,
	msgsRelayed *atomic.Uint64,
	metrics MetricsReporter,
	id Identity,
	opts RunOptions,
	committer *commitAggregator,
) error {
	pending := committer.pendingRecords()
	records := committer.drain()
	if len(records) == 0 {
		return nil
	}
	if err := retryStage(ctx, log, "commit", opts.Retry.Commit, errorsSeen, metrics, id, opts, func() error {
		return consumer.CommitRecords(ctx, records...)
	}); err != nil {
		return fmt.Errorf("commit from-side offsets: %w", err)
	}
	for i := 0; i < pending; i++ {
		metrics.IncMessages(id)
	}
	msgsRelayed.Add(uint64(pending))
	return nil
}
