package relay

import (
	"crypto/sha256"
	"encoding/binary"
	"sync"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka record header keys for the source coordinates of a relayed message. Downstream consumers
// can treat (cluster, topic, partition, offset) as a stable idempotency key across at-least-once
// delivery, or dedupe using [HeaderCourseHash] alone.
const (
	HeaderCourseHash      = "bifrost.course.hash"
	HeaderSourceCluster   = "bifrost.source.cluster"
	HeaderSourceTopic     = "bifrost.source.topic"
	HeaderSourcePartition = "bifrost.source.partition"
	HeaderSourceOffset    = "bifrost.source.offset"
)

const courseHashPreimageVersion byte = 1

var courseHashPreimagePool = sync.Pool{
	New: func() any {
		s := make([]byte, 0, 256)
		return &s
	},
}

// NormalizedSourceCoord returns partition and offset for header encoding (negative → 0).
func NormalizedSourceCoord(r *kgo.Record) (partition int32, offset int64) {
	partition = r.Partition
	if partition < 0 {
		partition = 0
	}
	offset = r.Offset
	if offset < 0 {
		offset = 0
	}
	return partition, offset
}

func encodePartitionOffset(partition int32, offset int64) (part [4]byte, off [8]byte) {
	if partition < 0 {
		partition = 0
	}
	if offset < 0 {
		offset = 0
	}
	binary.BigEndian.PutUint32(part[:], uint32(partition)) // #nosec G115 -- clamped ≥ 0; matches legacy relay encoding
	binary.BigEndian.PutUint64(off[:], uint64(offset))     // #nosec G115 -- clamped ≥ 0; matches legacy relay encoding
	return part, off
}

// CourseHash returns the full SHA-256 digest used as bifrost.course.hash value for the given
// source coordinates (opaque dedupe fingerprint).
func CourseHash(id Identity, topic string, partition int32, offset int64) [32]byte {
	p := courseHashPreimagePool.Get().(*[]byte)
	buf := *p
	buf = courseHashPreimage(buf[:0], id, topic, partition, offset)
	sum := sha256.Sum256(buf)
	*p = buf
	courseHashPreimagePool.Put(p)
	return sum
}

func courseHashPreimage(dst []byte, id Identity, topic string, partition int32, offset int64) []byte {
	dst = append(dst, courseHashPreimageVersion)
	dst = append(dst, id.FromCluster...)
	dst = append(dst, 0)
	dst = append(dst, topic...)
	dst = append(dst, 0)
	p, o := encodePartitionOffset(partition, offset)
	dst = append(dst, p[:]...)
	dst = append(dst, o[:]...)
	return dst
}

// AppendCourseHashHeader appends bifrost.course.hash with a full SHA-256 over the canonical preimage.
func AppendCourseHashHeader(hdrs []kgo.RecordHeader, id Identity, topic string, partition int32, offset int64) []kgo.RecordHeader {
	sum := CourseHash(id, topic, partition, offset)
	val := append([]byte(nil), sum[:]...)
	return append(hdrs, kgo.RecordHeader{
		Key:   HeaderCourseHash,
		Value: val,
	})
}

// AppendSourceHeaders appends bifrost.source.* headers describing the consumed source record.
// Partition is encoded as 4 bytes big-endian; offset as 8 bytes big-endian (unsigned).
func AppendSourceHeaders(hdrs []kgo.RecordHeader, id Identity, topic string, partition int32, offset int64) []kgo.RecordHeader {
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourceCluster,
		Value: []byte(id.FromCluster),
	})
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourceTopic,
		Value: []byte(topic),
	})
	p, o := encodePartitionOffset(partition, offset)
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourcePartition,
		Value: p[:],
	})
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourceOffset,
		Value: o[:],
	})
	return hdrs
}
