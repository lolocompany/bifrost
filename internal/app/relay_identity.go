package app

import (
	"github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/domain/relay"
)

func relayIdentityFromBridge(bridgeCfg config.Bridge) relay.Identity {
	return relay.Identity{
		BridgeName:  bridgeCfg.Name,
		FromCluster: bridgeCfg.From.Cluster,
		FromTopic:   bridgeCfg.From.Topic,
		ToCluster:   bridgeCfg.To.Cluster,
		ToTopic:     bridgeCfg.To.Topic,
	}
}
