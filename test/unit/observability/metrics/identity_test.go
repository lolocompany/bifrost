package metrics_test

import (
	bifrostconfig "github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/domain/relay"
)

func relayIdentityFromBridge(bridgeCfg bifrostconfig.Bridge) relay.Identity {
	return relay.Identity{
		BridgeName:  bridgeCfg.Name,
		FromCluster: bridgeCfg.From.Cluster,
		FromTopic:   bridgeCfg.From.Topic,
		ToCluster:   bridgeCfg.To.Cluster,
		ToTopic:     bridgeCfg.To.Topic,
	}
}
