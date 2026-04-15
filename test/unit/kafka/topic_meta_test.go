package kafka_test

import (
	"context"
	"testing"

	"github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestTopicPartitionCount_validation(t *testing.T) {
	t.Parallel()
	ctx := context.Background()
	_, err := kafka.TopicPartitionCount(ctx, nil, "t")
	if err == nil {
		t.Fatal("expected error for nil client")
	}
	cl, err := kgo.NewClient()
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	defer cl.Close()
	_, err = kafka.TopicPartitionCount(ctx, cl, "")
	if err == nil {
		t.Fatal("expected error for empty topic")
	}
	_, err = kafka.TopicPartitionCount(ctx, cl, "   ")
	if err == nil {
		t.Fatal("expected error for whitespace-only topic")
	}
}
