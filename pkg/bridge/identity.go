package bridge

import bifrostconfig "github.com/lolocompany/bifrost/pkg/config"

// labelNames is the fixed order of labels for per-bridge relay metrics.
// Keep in sync with Identity.LabelValues.
var labelNames = []string{
	"bridge",
	"from_kafka_cluster",
	"from_topic",
	"to_kafka_cluster",
	"to_topic",
}

// LabelNames returns metric label names in fixed order.
func LabelNames() []string {
	return append([]string(nil), labelNames...)
}

// Identity identifies one bridge for Prometheus labels (bounded cardinality: one series set per configured bridge).
type Identity struct {
	BridgeName  string
	FromCluster string
	FromTopic   string
	ToCluster   string
	ToTopic     string
}

// IdentityFrom builds identity from config (must match YAML bridge names and cluster keys).
func IdentityFrom(b bifrostconfig.Bridge) Identity {
	return Identity{
		BridgeName:  b.Name,
		FromCluster: b.From.Cluster,
		FromTopic:   b.From.Topic,
		ToCluster:   b.To.Cluster,
		ToTopic:     b.To.Topic,
	}
}

// LabelValues returns Prometheus label values in LabelNames order.
func (id Identity) LabelValues() []string {
	return []string{
		id.BridgeName,
		id.FromCluster,
		id.FromTopic,
		id.ToCluster,
		id.ToTopic,
	}
}
