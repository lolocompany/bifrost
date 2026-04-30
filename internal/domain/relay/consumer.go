package relay

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// FetchResult is the consumed batch surface used by RunWithClients.
type FetchResult interface {
	Err() error
	NumRecords() int
	Records() []*kgo.Record
}

// ConsumerClient is the minimal consumer surface required by the relay loop.
type ConsumerClient interface {
	PollFetches(context.Context) FetchResult
	CommitRecords(context.Context, ...*kgo.Record) error
}
