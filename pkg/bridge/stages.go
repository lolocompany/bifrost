package bridge

// Relay stage labels used by metrics and retry logging.
const (
	StagePoll    = "poll"
	StageProduce = "produce"
	StageCommit  = "commit"
	StageRoute   = "route"
)

// RelayState labels used by utilization counters.
const (
	RelayStateBusy = "busy"
	RelayStateIdle = "idle"
)

