package relay

// Metrics is the callback surface used by Run to report relay activity.
type Metrics interface {
	IncMessages(id Identity)
	IncErrors(id Identity, stage string)
	ObserveProduceDuration(id Identity, seconds float64)
	AddConsumerSeconds(id Identity, state string, seconds float64)
	AddProducerSeconds(id Identity, state string, seconds float64)
}
