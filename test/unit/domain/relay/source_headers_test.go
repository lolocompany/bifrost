package relay_test

import (
	"bytes"
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
	part, off := relay.NormalizedSourceCoord(r)
	var base []kgo.RecordHeader
	got := append(append([]kgo.RecordHeader{}, relay.AppendSourceHeaders(base, id, r.Topic, part, off)...), r.Headers...)

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

func TestCourseHash_deterministic(t *testing.T) {
	id := relay.Identity{FromCluster: "c1"}
	topic := "t1"
	var part int32 = 7
	var off int64 = 99
	h1 := relay.CourseHash(id, topic, part, off)
	h2 := relay.CourseHash(id, topic, part, off)
	if h1 != h2 {
		t.Fatalf("hash mismatch")
	}
	if len(h1) != 32 {
		t.Fatalf("len %d want 32", len(h1))
	}
}

func TestCourseHash_distinctInputs(t *testing.T) {
	id := relay.Identity{FromCluster: "c1"}
	h1 := relay.CourseHash(id, "t", 0, 0)
	h2 := relay.CourseHash(id, "u", 0, 0)
	if bytes.Equal(h1[:], h2[:]) {
		t.Fatal("expected different hashes")
	}
}
