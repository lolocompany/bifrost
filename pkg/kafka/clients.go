// Package kafka constructs franz-go clients for named clusters.
package kafka

import (
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/lolocompany/bifrost/pkg/config"
)

// NewConsumerForBridge returns a consumer-group client for one bridge (one from-side topic).
// Optional extra kgo options are applied last (e.g. [kgo.FetchMaxPartitionBytes] for large records).
func NewConsumerForBridge(env *config.Cluster, group string, topic string, hooks []kgo.Hook, extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := ClientOpts(env)
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
// Optional extra kgo options are applied last (e.g. [kgo.ProducerBatchMaxBytes] for large records).
func NewProducer(env *config.Cluster, hooks []kgo.Hook, extra ...kgo.Opt) (*kgo.Client, error) {
	base, err := ClientOpts(env)
	if err != nil {
		return nil, err
	}
	cust, err := ProducerClusterOpts(env)
	if err != nil {
		return nil, err
	}
	opts := append(base, cust...)
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
