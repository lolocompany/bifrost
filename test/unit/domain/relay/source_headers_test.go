package relay_test

import (
	"encoding/binary"
	"testing"

	"github.com/lolocompany/bifrost/internal/domain/relay"
	"github.com/twmb/franz-go/pkg/kgo"
)

func TestAppendSourceHeaders(t *testing.T) {
	id := relay.Identity{
		BridgeName:  "b",
		FromCluster: "east",
		FromTopic:   "in",
		ToCluster:   "west",
		ToTopic:     "out",
	}
	r := &kgo.Record{
		Topic:     "in",
		Partition: 3,
		Offset:    42,
		Headers: []kgo.RecordHeader{
			{Key: "x", Value: []byte("y")},
		},
	}

	var base []kgo.RecordHeader
	got := append(append([]kgo.RecordHeader{}, relay.AppendSourceHeaders(base, id, r)...), r.Headers...)

	if len(got) != 5 {
		t.Fatalf("headers: len %d want 5", len(got))
	}
	if string(got[0].Key) != relay.HeaderSourceCluster || string(got[0].Value) != "east" {
		t.Fatalf("cluster header: %+v", got[0])
	}
	if string(got[1].Key) != relay.HeaderSourceTopic || string(got[1].Value) != "in" {
		t.Fatalf("topic header: %+v", got[1])
	}
	if string(got[2].Key) != relay.HeaderSourcePartition {
		t.Fatalf("partition key: %s", got[2].Key)
	}
	if binary.BigEndian.Uint32(got[2].Value) != 3 {
		t.Fatalf("partition value: %v", got[2].Value)
	}
	if string(got[3].Key) != relay.HeaderSourceOffset {
		t.Fatalf("offset key: %s", got[3].Key)
	}
	if binary.BigEndian.Uint64(got[3].Value) != 42 {
		t.Fatalf("offset value: %v", got[3].Value)
	}
	if string(got[4].Key) != "x" || string(got[4].Value) != "y" {
		t.Fatalf("original header: %+v", got[4])
	}
}
