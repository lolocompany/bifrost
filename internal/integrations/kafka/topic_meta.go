package kafka

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kgo"
)

// TopicPartitionCount returns the number of partitions for topic using a Kafka metadata request.
func TopicPartitionCount(ctx context.Context, cl *kgo.Client, topic string) (int, error) {
	if cl == nil {
		return 0, errors.New("kafka client is nil")
	}
	topic = strings.TrimSpace(topic)
	if topic == "" {
		return 0, errors.New("topic is empty")
	}
	adm := kadm.NewClient(cl)
	tds, err := adm.ListTopics(ctx, topic)
	if err != nil {
		return 0, fmt.Errorf("list topics: %w", err)
	}
	td, ok := tds[topic]
	if !ok {
		return 0, fmt.Errorf("metadata: topic %q not in response", topic)
	}
	if td.Err != nil {
		return 0, fmt.Errorf("topic %q: %w", topic, td.Err)
	}
	n := len(td.Partitions)
	if n < 1 {
		return 0, fmt.Errorf("topic %q has no partitions in metadata", topic)
	}
	return n, nil
}
