package metrics

import (
	bifrostconfig "github.com/lolocompany/bifrost/pkg/config"
)

// BridgeLabelNames is the fixed order of labels for per-bridge relay metrics (forward, latency).
// Keep in sync with BridgeIdentity.LabelValues.
var BridgeLabelNames = []string{
	"bridge",
	"from_cluster",
	"from_topic",
	"to_cluster",
	"to_topic",
}

// BridgeIdentity identifies one bridge for Prometheus labels (bounded cardinality: one series set per configured bridge).
type BridgeIdentity struct {
	BridgeName  string
	FromCluster string
	FromTopic   string
	ToCluster   string
	ToTopic     string
}

// BridgeIdentityFrom builds identity from config (must match YAML bridge names and cluster keys).
func BridgeIdentityFrom(b bifrostconfig.Bridge) BridgeIdentity {
	return BridgeIdentity{
		BridgeName:  b.Name,
		FromCluster: b.From.Cluster,
		FromTopic:   b.From.Topic,
		ToCluster:   b.To.Cluster,
		ToTopic:     b.To.Topic,
	}
}

// LabelValues returns Prometheus label values in BridgeLabelNames order.
func (id BridgeIdentity) LabelValues() []string {
	return []string{
		id.BridgeName,
		id.FromCluster,
		id.FromTopic,
		id.ToCluster,
		id.ToTopic,
	}
}
