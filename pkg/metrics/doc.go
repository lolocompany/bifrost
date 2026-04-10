// Package metrics registers Prometheus collectors for bifrost.
//
// Metric names follow https://prometheus.io/docs/practices/naming/ and OpenMetrics conventions:
//   - Counters use the _total suffix.
//   - Histograms of duration use _duration_seconds (base unit seconds).
//   - Histograms of size use _bytes where applicable.
//   - Gauges include a unit suffix where meaningful (e.g. _timestamp_seconds for Unix time).
//   - go_* and process_* series come from client_golang collectors with their standard names.
package metrics
