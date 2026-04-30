package relay

import (
	"context"

	"github.com/twmb/franz-go/pkg/kgo"
)

// ProducerClient is the minimal producer surface required by the relay loop.
type ProducerClient interface {
	Produce(context.Context, *kgo.Record, func(*kgo.Record, error))
}
