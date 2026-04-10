package kafka

import (
	"context"
	"errors"
	"fmt"
	"sort"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

// EnsureTopics creates each named topic if it does not already exist when autoCreateTopics is true.
// When autoCreateTopics is false, it does nothing and returns no error.
// Partitions and replication use broker defaults (-1). TOPIC_ALREADY_EXISTS is ignored.
// Created lists topic names that were created (not already present).
func EnsureTopics(ctx context.Context, cl *kgo.Client, autoCreateTopics bool, topics []string) (created []string, err error) {
	if !autoCreateTopics {
		return nil, nil
	}
	if cl == nil {
		return nil, errors.New("kafka client is nil")
	}
	topics = uniqueSortedNonEmpty(topics)
	if len(topics) == 0 {
		return nil, nil
	}
	adm := kadm.NewClient(cl)
	const (
		partitions        int32 = -1
		replicationFactor int16 = -1
	)
	resp, err := adm.CreateTopics(ctx, partitions, replicationFactor, nil, topics...)
	if err != nil {
		return nil, fmt.Errorf("create topics: %w", err)
	}
	for _, r := range resp.Sorted() {
		if r.Err != nil {
			if errors.Is(r.Err, kerr.TopicAlreadyExists) {
				continue
			}
			return nil, fmt.Errorf("topic %q: %w", r.Topic, r.Err)
		}
		created = append(created, r.Topic)
	}
	return created, nil
}

func uniqueSortedNonEmpty(xs []string) []string {
	seen := make(map[string]struct{}, len(xs))
	for _, x := range xs {
		if x == "" {
			continue
		}
		seen[x] = struct{}{}
	}
	out := make([]string, 0, len(seen))
	for x := range seen {
		out = append(out, x)
	}
	sort.Strings(out)
	return out
}
