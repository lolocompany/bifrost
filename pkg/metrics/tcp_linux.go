//go:build linux

package metrics

import (
	"os"

	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/procfs"
)

// TCPMetricNames lists every Prometheus metric name emitted by tcpNamespaceCollector.
// Use a single bifrost_tcp_ prefix; snmp_* from /proc/net/snmp Tcp, netstat_* from TcpExt — never "tcp_tcp".
var TCPMetricNames = []string{
	tcpNameRetransSegments,
	tcpNameActiveOpens,
	tcpNamePassiveOpens,
	tcpNameAttemptFails,
	tcpNameEstabResets,
	tcpNameCurrentEstablished,
	tcpNameInSegments,
	tcpNameOutSegments,
	tcpNameInErrors,
	tcpNameOutResets,
	tcpNameNetstatLostRetransmit,
	tcpNameNetstatFastRetrans,
	tcpNameNetstatSlowStartRetrans,
	tcpNameNetstatRetransFail,
	tcpNameNetstatSynRetrans,
	tcpNameNetstatTimeouts,
	tcpNameNetstatSpuriousRTOs,
	tcpNameNetstatBacklogDrop,
	tcpNameNetstatAbortOnTimeout,
}

const (
	tcpNameRetransSegments         = "bifrost_tcp_retrans_segments"
	tcpNameActiveOpens             = "bifrost_tcp_active_opens"
	tcpNamePassiveOpens            = "bifrost_tcp_passive_opens"
	tcpNameAttemptFails            = "bifrost_tcp_attempt_fails"
	tcpNameEstabResets             = "bifrost_tcp_estab_resets"
	tcpNameCurrentEstablished      = "bifrost_tcp_current_established"
	tcpNameInSegments              = "bifrost_tcp_in_segments"
	tcpNameOutSegments             = "bifrost_tcp_out_segments"
	tcpNameInErrors                = "bifrost_tcp_in_errors"
	tcpNameOutResets               = "bifrost_tcp_out_resets"
	tcpNameNetstatLostRetransmit   = "bifrost_tcp_netstat_lost_retransmit"
	tcpNameNetstatFastRetrans      = "bifrost_tcp_netstat_fast_retrans"
	tcpNameNetstatSlowStartRetrans = "bifrost_tcp_netstat_slow_start_retrans"
	tcpNameNetstatRetransFail      = "bifrost_tcp_netstat_retrans_fail"
	tcpNameNetstatSynRetrans       = "bifrost_tcp_netstat_syn_retrans"
	tcpNameNetstatTimeouts         = "bifrost_tcp_netstat_timeouts"
	tcpNameNetstatSpuriousRTOs     = "bifrost_tcp_netstat_spurious_rtos"
	tcpNameNetstatBacklogDrop      = "bifrost_tcp_netstat_backlog_drop"
	tcpNameNetstatAbortOnTimeout   = "bifrost_tcp_netstat_abort_on_timeout"
)

// NewTCPNamespaceCollector returns a collector that exposes TCP counters from the
// process network namespace via /proc/<pid>/net/snmp and /proc/<pid>/net/netstat.
// These are kernel counters (retransmissions, segments, etc.) for the whole netns, not per-socket.
func NewTCPNamespaceCollector() (prometheus.Collector, error) {
	return newTCPNamespaceCollector(), nil
}

type tcpNamespaceCollector struct {
	// snmp Tcp (partial)
	descRetransSegs  *prometheus.Desc
	descActiveOpens  *prometheus.Desc
	descPassiveOpens *prometheus.Desc
	descAttemptFails *prometheus.Desc
	descEstabResets  *prometheus.Desc
	descCurrEstab    *prometheus.Desc
	descInSegs       *prometheus.Desc
	descOutSegs      *prometheus.Desc
	descInErrs       *prometheus.Desc
	descOutRsts      *prometheus.Desc
	// netstat TcpExt (retrans and related)
	descTCPLostRetransmit   *prometheus.Desc
	descTCPFastRetrans      *prometheus.Desc
	descTCPSlowStartRetrans *prometheus.Desc
	descTCPRetransFail      *prometheus.Desc
	descTCPSynRetrans       *prometheus.Desc
	descTCPTimeouts         *prometheus.Desc
	descTCPSpuriousRTOs     *prometheus.Desc
	descTCPBacklogDrop      *prometheus.Desc
	descTCPAbortOnTimeout   *prometheus.Desc
}

func newTCPNamespaceCollector() *tcpNamespaceCollector {
	const ns = "Values from the Linux network namespace of this process (/proc/self/net/snmp, netstat)."
	return &tcpNamespaceCollector{
		descRetransSegs: prometheus.NewDesc(
			tcpNameRetransSegments,
			"Tcp:RetransSegs — TCP segments retransmitted. "+ns,
			nil, nil,
		),
		descActiveOpens: prometheus.NewDesc(
			tcpNameActiveOpens,
			"Tcp:ActiveOpens — TCP connections opened by active opens. "+ns,
			nil, nil,
		),
		descPassiveOpens: prometheus.NewDesc(
			tcpNamePassiveOpens,
			"Tcp:PassiveOpens — TCP connections opened by passive opens. "+ns,
			nil, nil,
		),
		descAttemptFails: prometheus.NewDesc(
			tcpNameAttemptFails,
			"Tcp:AttemptFails — failed connection attempts. "+ns,
			nil, nil,
		),
		descEstabResets: prometheus.NewDesc(
			tcpNameEstabResets,
			"Tcp:EstabResets — resets of established connections. "+ns,
			nil, nil,
		),
		descCurrEstab: prometheus.NewDesc(
			tcpNameCurrentEstablished,
			"Tcp:CurrEstab — currently established TCP connections. "+ns,
			nil, nil,
		),
		descInSegs: prometheus.NewDesc(
			tcpNameInSegments,
			"Tcp:InSegs — TCP segments received. "+ns,
			nil, nil,
		),
		descOutSegs: prometheus.NewDesc(
			tcpNameOutSegments,
			"Tcp:OutSegs — TCP segments sent. "+ns,
			nil, nil,
		),
		descInErrs: prometheus.NewDesc(
			tcpNameInErrors,
			"Tcp:InErrs — TCP receive errors. "+ns,
			nil, nil,
		),
		descOutRsts: prometheus.NewDesc(
			tcpNameOutResets,
			"Tcp:OutRsts — TCP segments sent containing RST. "+ns,
			nil, nil,
		),
		descTCPLostRetransmit: prometheus.NewDesc(
			tcpNameNetstatLostRetransmit,
			"TcpExt:TCPLostRetransmit — lost packets after retransmit. "+ns,
			nil, nil,
		),
		descTCPFastRetrans: prometheus.NewDesc(
			tcpNameNetstatFastRetrans,
			"TcpExt:TCPFastRetrans — fast retransmit recoveries. "+ns,
			nil, nil,
		),
		descTCPSlowStartRetrans: prometheus.NewDesc(
			tcpNameNetstatSlowStartRetrans,
			"TcpExt:TCPSlowStartRetrans — slow start retransmits. "+ns,
			nil, nil,
		),
		descTCPRetransFail: prometheus.NewDesc(
			tcpNameNetstatRetransFail,
			"TcpExt:TCPRetransFail — retransmit failures. "+ns,
			nil, nil,
		),
		descTCPSynRetrans: prometheus.NewDesc(
			tcpNameNetstatSynRetrans,
			"TcpExt:TCPSynRetrans — SYN retransmits. "+ns,
			nil, nil,
		),
		descTCPTimeouts: prometheus.NewDesc(
			tcpNameNetstatTimeouts,
			"TcpExt:TCPTimeouts — TCP timeouts. "+ns,
			nil, nil,
		),
		descTCPSpuriousRTOs: prometheus.NewDesc(
			tcpNameNetstatSpuriousRTOs,
			"TcpExt:TCPSpuriousRTOs — spurious RTOs. "+ns,
			nil, nil,
		),
		descTCPBacklogDrop: prometheus.NewDesc(
			tcpNameNetstatBacklogDrop,
			"TcpExt:TCPBacklogDrop — TCP backlog drops. "+ns,
			nil, nil,
		),
		descTCPAbortOnTimeout: prometheus.NewDesc(
			tcpNameNetstatAbortOnTimeout,
			"TcpExt:TCPAbortOnTimeout — connections aborted on timeout. "+ns,
			nil, nil,
		),
	}
}

func (c *tcpNamespaceCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, d := range []*prometheus.Desc{
		c.descRetransSegs, c.descActiveOpens, c.descPassiveOpens, c.descAttemptFails,
		c.descEstabResets, c.descCurrEstab, c.descInSegs, c.descOutSegs, c.descInErrs, c.descOutRsts,
		c.descTCPLostRetransmit, c.descTCPFastRetrans, c.descTCPSlowStartRetrans, c.descTCPRetransFail,
		c.descTCPSynRetrans, c.descTCPTimeouts, c.descTCPSpuriousRTOs, c.descTCPBacklogDrop, c.descTCPAbortOnTimeout,
	} {
		ch <- d
	}
}

func (c *tcpNamespaceCollector) Collect(ch chan<- prometheus.Metric) {
	fs, err := procfs.NewDefaultFS()
	if err != nil {
		return
	}
	p, err := fs.Proc(os.Getpid())
	if err != nil {
		return
	}
	snmp, err := p.Snmp()
	if err != nil {
		return
	}
	ns, err := p.Netstat()
	if err != nil {
		// still emit snmp-only metrics
		c.emitSnmp(ch, snmp)
		return
	}
	c.emitSnmp(ch, snmp)
	c.emitNetstat(ch, ns)
}

func (c *tcpNamespaceCollector) emitSnmp(ch chan<- prometheus.Metric, snmp procfs.ProcSnmp) {
	t := snmp.Tcp
	emit(ch, c.descRetransSegs, t.RetransSegs)
	emit(ch, c.descActiveOpens, t.ActiveOpens)
	emit(ch, c.descPassiveOpens, t.PassiveOpens)
	emit(ch, c.descAttemptFails, t.AttemptFails)
	emit(ch, c.descEstabResets, t.EstabResets)
	emit(ch, c.descCurrEstab, t.CurrEstab)
	emit(ch, c.descInSegs, t.InSegs)
	emit(ch, c.descOutSegs, t.OutSegs)
	emit(ch, c.descInErrs, t.InErrs)
	emit(ch, c.descOutRsts, t.OutRsts)
}

func (c *tcpNamespaceCollector) emitNetstat(ch chan<- prometheus.Metric, ns procfs.ProcNetstat) {
	x := ns.TcpExt
	emit(ch, c.descTCPLostRetransmit, x.TCPLostRetransmit)
	emit(ch, c.descTCPFastRetrans, x.TCPFastRetrans)
	emit(ch, c.descTCPSlowStartRetrans, x.TCPSlowStartRetrans)
	emit(ch, c.descTCPRetransFail, x.TCPRetransFail)
	emit(ch, c.descTCPSynRetrans, x.TCPSynRetrans)
	emit(ch, c.descTCPTimeouts, x.TCPTimeouts)
	emit(ch, c.descTCPSpuriousRTOs, x.TCPSpuriousRTOs)
	emit(ch, c.descTCPBacklogDrop, x.TCPBacklogDrop)
	emit(ch, c.descTCPAbortOnTimeout, x.TCPAbortOnTimeout)
}

func emit(ch chan<- prometheus.Metric, desc *prometheus.Desc, v *float64) {
	if v == nil {
		return
	}
	ch <- prometheus.MustNewConstMetric(desc, prometheus.GaugeValue, *v)
}
