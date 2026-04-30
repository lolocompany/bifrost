package relay

import (
	"encoding/binary"

	"github.com/twmb/franz-go/pkg/kgo"
)

// Kafka record header keys for the source coordinates of a relayed message. Downstream consumers
// can treat (cluster, topic, partition, offset) as a stable idempotency key across at-least-once
// delivery. Values are always set by the bridge; headers are listed first, then optional bridge
// extra_headers from config, then any headers copied from the source record.
const (
	HeaderSourceCluster   = "bifrost.source.cluster"
	HeaderSourceTopic     = "bifrost.source.topic"
	HeaderSourcePartition = "bifrost.source.partition"
	HeaderSourceOffset    = "bifrost.source.offset"
)

// AppendSourceHeaders appends bifrost.source.* headers describing the consumed source record.
// Partition is encoded as 4 bytes big-endian; offset as 8 bytes big-endian (unsigned).
func AppendSourceHeaders(hdrs []kgo.RecordHeader, id Identity, r *kgo.Record) []kgo.RecordHeader {
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourceCluster,
		Value: []byte(id.FromCluster),
	})
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourceTopic,
		Value: []byte(r.Topic),
	})
	part := r.Partition
	if part < 0 {
		part = 0
	}
	var partBuf [4]byte
	binary.BigEndian.PutUint32(partBuf[:], uint32(part))
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourcePartition,
		Value: partBuf[:],
	})
	off := r.Offset
	if off < 0 {
		off = 0
	}
	var offBuf [8]byte
	binary.BigEndian.PutUint64(offBuf[:], uint64(off))
	hdrs = append(hdrs, kgo.RecordHeader{
		Key:   HeaderSourceOffset,
		Value: offBuf[:],
	})
	return hdrs
}
