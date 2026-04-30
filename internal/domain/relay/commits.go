package relay

import (
	"fmt"
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

type commitContext struct {
	consumer    ConsumerClient
	msgsRelayed *atomic.Uint64
	committer   *commitAggregator
	retry       retryContext
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

func flushCommits(cc commitContext) error {
	pending := cc.committer.pendingRecords()
	records := cc.committer.drain()
	if len(records) == 0 {
		return nil
	}
	retry := cc.retry
	retry.stage = StageCommit
	retry.cfg = cc.retry.opts.Retry.Commit
	if err := retryStage(retry, func() error {
		return cc.consumer.CommitRecords(retry.ctx, records...)
	}); err != nil {
		return fmt.Errorf("commit from-side offsets: %w", err)
	}
	for i := 0; i < pending; i++ {
		cc.retry.metrics.IncMessages(cc.retry.id)
	}
	if pending < 0 {
		return fmt.Errorf("pending committed records cannot be negative: %d", pending)
	}
	cc.msgsRelayed.Add(uint64(pending))
	return nil
}
