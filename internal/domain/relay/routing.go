package relay

import (
	"errors"
	"fmt"

	"github.com/twmb/franz-go/pkg/kgo"
)

type topicMismatchError struct {
	got  string
	want string
}

func (e topicMismatchError) Error() string {
	return fmt.Sprintf("unexpected topic %q (want %q)", e.got, e.want)
}

func errTopic(err error) string {
	var topicErr topicMismatchError
	if errors.As(err, &topicErr) {
		return topicErr.got
	}
	return ""
}

type topicPartition struct {
	topic     string
	partition int32
}

func partitionBatches(id Identity, records []*kgo.Record, batchSize int) ([][]*kgo.Record, error) {
	if batchSize <= 0 {
		batchSize = 1
	}
	grouped := make(map[topicPartition][]*kgo.Record)
	order := make([]topicPartition, 0)
	for _, r := range records {
		if r.Topic != id.FromTopic {
			return nil, topicMismatchError{got: r.Topic, want: id.FromTopic}
		}
		key := topicPartition{topic: r.Topic, partition: r.Partition}
		if _, ok := grouped[key]; !ok {
			order = append(order, key)
		}
		grouped[key] = append(grouped[key], r)
	}

	batches := make([][]*kgo.Record, 0, len(records))
	for _, key := range order {
		partitionRecords := grouped[key]
		for start := 0; start < len(partitionRecords); start += batchSize {
			end := start + batchSize
			if end > len(partitionRecords) {
				end = len(partitionRecords)
			}
			batches = append(batches, partitionRecords[start:end])
		}
	}
	return batches, nil
}
