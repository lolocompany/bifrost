// Package kafka constructs franz-go clients for named clusters.
package kafka

import (
	"context"
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lolocompany/bifrost/internal/config"
)

// PingBroker checks that at least one seed or discovered broker responds to a metadata request.
// Use [WithPingTimeout] (or your own deadline) so startup fails instead of hanging in fetch loops
// when brokers are unreachable.
func PingBroker(ctx context.Context, cl *kgo.Client) error {
	if cl == nil {
		return errors.New("kafka client is nil")
	}
	return cl.Ping(ctx)
}

// WithPingTimeout returns a child context suitable for [PingBroker]. It uses cluster.client.dial_timeout
// when set and valid; otherwise it uses the config default ping timeout.
func WithPingTimeout(parent context.Context, env *config.Cluster) (context.Context, context.CancelFunc, error) {
	if parent == nil {
		return nil, nil, errors.New("parent context is nil")
	}
	d := config.DefaultPingTimeout
	if env != nil {
		parsed, err := env.Client.PingTimeoutDuration()
		if err != nil {
			return nil, nil, fmt.Errorf("client.dial_timeout: %w", err)
		}
		d = parsed
	}
	pingCtx, cancel := context.WithTimeout(parent, d)
	return pingCtx, cancel, nil
}

// NewConsumerForBridge returns a consumer-group client for one bridge (one from-side topic).
// recordTCPDialSeconds is passed to [ClientOpts]; use nil unless recording bifrost_tcp_connect_duration_seconds.
// Optional extra kgo options are applied last (e.g. [kgo.FetchMaxPartitionBytes] for large records).
func NewConsumerForBridge(env *config.Cluster, group string, topic string, hooks []kgo.Hook, recordTCPDialSeconds func(float64), extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := ClientOpts(env, recordTCPDialSeconds)
	if err != nil {
		return nil, err
	}
	cust, err := ConsumerClusterOpts(env)
	if err != nil {
		return nil, err
	}
	opts := append(base, cust...)
	opts = append(opts,
		kgo.ConsumerGroup(group),
		kgo.ConsumeTopics(topic),
		kgo.ConsumeResetOffset(kgo.NewOffset().AtStart()),
		kgo.DisableAutoCommit(),
	)
	if len(hooks) > 0 {
		opts = append(opts, kgo.WithHooks(hooks...))
	}
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create consumer: %w", err)
	}
	return cl, nil
}

// NewProducer returns a producer client for a cluster (shared across bridges that share the same to-side cluster).
// recordTCPDialSeconds is passed to [ClientOpts]; use nil unless recording bifrost_tcp_connect_duration_seconds.
// Optional extra kgo options are applied last (e.g. [kgo.ProducerBatchMaxBytes] for large records).
func NewProducer(env *config.Cluster, hooks []kgo.Hook, recordTCPDialSeconds func(float64), extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := ClientOpts(env, recordTCPDialSeconds)
	if err != nil {
		return nil, err
	}
	cust, err := ProducerClusterOpts(env)
	if err != nil {
		return nil, err
	}
	opts := append(base, cust...)
	opts = append(opts, kgo.RecordPartitioner(kgo.ManualPartitioner()))
	if len(hooks) > 0 {
		opts = append(opts, kgo.WithHooks(hooks...))
	}
	opts = append(opts, extra...)
	cl, err := kgo.NewClient(opts...)
	if err != nil {
		return nil, fmt.Errorf("create producer: %w", err)
	}
	return cl, nil
}
