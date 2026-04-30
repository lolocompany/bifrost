package kafka

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/lolocompany/bifrost/internal/config"
)

// ClientOpts returns franz-go options shared by producer and consumer clients for a cluster
// (brokers, TLS, SASL, and cluster.client).
//
// If recordTCPDialSeconds is non-nil, a custom [kgo.Dialer] is installed that measures TCP-only
// connect time (until the TCP socket is established) and calls recordTCPDialSeconds with that
// duration in seconds before the TLS handshake. This matches bifrost_tcp_connect_duration_seconds.
// Pass nil to use franz-go's default dial path (no TCP-only timing callback).
func ClientOpts(env *config.Cluster, recordTCPDialSeconds func(float64)) ([]kgo.Opt, error) {
	if env == nil {
		return nil, errors.New("cluster is nil")
	}
	var opts []kgo.Opt
	opts = append(opts, kgo.SeedBrokers(env.Brokers...))

	tlsCfg, err := tlsConfigFrom(&env.TLS)
	if err != nil {
		return nil, err
	}

	dialTimeout, err := env.Client.DialTimeoutDuration()
	if err != nil {
		return nil, fmt.Errorf("client.dial_timeout: %w", err)
	}

	if recordTCPDialSeconds != nil {
		opts = append(opts, kgo.Dialer(func(ctx context.Context, network, host string) (net.Conn, error) {
			return dialBrokerConn(ctx, network, host, tlsCfg, dialTimeout, recordTCPDialSeconds)
		}))
	} else if tlsCfg != nil {
		opts = append(opts, kgo.DialTLSConfig(tlsCfg))
	}

	mech, err := saslMechanism(&env.SASL)
	if err != nil {
		return nil, err
	}
	if mech != nil {
		opts = append(opts, kgo.SASL(mech))
	}

	cl := &env.Client
	if id := strings.TrimSpace(cl.ClientID); id != "" {
		opts = append(opts, kgo.ClientID(id))
	}
	if recordTCPDialSeconds == nil {
		if d, ok, err := parseDurationField(cl.DialTimeout); err != nil {
			return nil, fmt.Errorf("client.dial_timeout: %w", err)
		} else if ok {
			opts = append(opts, kgo.DialTimeout(d))
		}
	}
	if d, ok, err := parseDurationField(cl.RequestTimeoutOverhead); err != nil {
		return nil, fmt.Errorf("client.request_timeout_overhead: %w", err)
	} else if ok {
		opts = append(opts, kgo.RequestTimeoutOverhead(d))
	}
	if cl.BrokerMaxWriteBytes != nil {
		opts = append(opts, kgo.BrokerMaxWriteBytes(*cl.BrokerMaxWriteBytes))
	}
	if cl.BrokerMaxReadBytes != nil {
		opts = append(opts, kgo.BrokerMaxReadBytes(*cl.BrokerMaxReadBytes))
	}

	return opts, nil
}

// dialBrokerConn dials the broker address: plain TCP, then optional TLS (same order as franz-go's
// built-in DialTLSConfig path). recordTCPDialSeconds is called with the TCP dial duration in seconds
// only after the TCP connection succeeds (before TLS handshake).
func dialBrokerConn(ctx context.Context, network, host string, tlsCfg *tls.Config, dialTimeout time.Duration, recordTCPDialSeconds func(float64)) (net.Conn, error) {
	nd := net.Dialer{Timeout: dialTimeout}
	start := time.Now()
	raw, err := nd.DialContext(ctx, network, host)
	if err != nil {
		return nil, err
	}
	recordTCPDialSeconds(time.Since(start).Seconds())
	if tlsCfg != nil {
		cfg := tlsCfg.Clone()
		if cfg.ServerName == "" {
			server, _, err := net.SplitHostPort(host)
			if err != nil {
				if cerr := raw.Close(); cerr != nil {
					return nil, fmt.Errorf("unable to split host:port for dialing: %w", errors.Join(err, cerr))
				}
				return nil, fmt.Errorf("unable to split host:port for dialing: %w", err)
			}
			cfg.ServerName = server
		}
		tlsConn := tls.Client(raw, cfg)
		if err := tlsConn.HandshakeContext(ctx); err != nil {
			if cerr := raw.Close(); cerr != nil {
				return nil, fmt.Errorf("tls handshake: %w", errors.Join(err, cerr))
			}
			return nil, err
		}
		return tlsConn, nil
	}
	return raw, nil
}

func parseDurationField(s string) (d time.Duration, set bool, err error) {
	return config.ParseOptionalDuration("duration", s)
}

// ConsumerClusterOpts returns franz-go options from cluster.consumer (fetch, group, isolation).
func ConsumerClusterOpts(env *config.Cluster) ([]kgo.Opt, error) {
	if env == nil {
		return nil, errors.New("cluster is nil")
	}
	co := &env.Consumer
	var opts []kgo.Opt

	if co.FetchMaxBytes != nil {
		opts = append(opts, kgo.FetchMaxBytes(*co.FetchMaxBytes))
	}
	if co.FetchMaxPartitionBytes != nil {
		opts = append(opts, kgo.FetchMaxPartitionBytes(*co.FetchMaxPartitionBytes))
	}
	if d, ok, err := parseDurationField(co.FetchMaxWait); err != nil {
		return nil, fmt.Errorf("consumer.fetch_max_wait: %w", err)
	} else if ok {
		opts = append(opts, kgo.FetchMaxWait(d))
	}
	if d, ok, err := parseDurationField(co.SessionTimeout); err != nil {
		return nil, fmt.Errorf("consumer.session_timeout: %w", err)
	} else if ok {
		opts = append(opts, kgo.SessionTimeout(d))
	}
	if d, ok, err := parseDurationField(co.HeartbeatInterval); err != nil {
		return nil, fmt.Errorf("consumer.heartbeat_interval: %w", err)
	} else if ok {
		opts = append(opts, kgo.HeartbeatInterval(d))
	}
	if d, ok, err := parseDurationField(co.RebalanceTimeout); err != nil {
		return nil, fmt.Errorf("consumer.rebalance_timeout: %w", err)
	} else if ok {
		opts = append(opts, kgo.RebalanceTimeout(d))
	}

	switch strings.ToLower(strings.TrimSpace(co.IsolationLevel)) {
	case "", "read_uncommitted":
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadUncommitted()))
	case "read_committed":
		opts = append(opts, kgo.FetchIsolationLevel(kgo.ReadCommitted()))
	default:
		return nil, fmt.Errorf("consumer.isolation_level: unsupported %q", co.IsolationLevel)
	}

	return opts, nil
}

// ProducerClusterOpts returns franz-go options from cluster.producer (acks, batching, compression).
func ProducerClusterOpts(env *config.Cluster) ([]kgo.Opt, error) {
	if env == nil {
		return nil, errors.New("cluster is nil")
	}
	p := &env.Producer
	var opts []kgo.Opt

	acks, err := requiredAcksFromString(p.RequiredAcks)
	if err != nil {
		return nil, err
	}
	opts = append(opts, kgo.RequiredAcks(acks))

	if p.BatchMaxBytes != nil {
		opts = append(opts, kgo.ProducerBatchMaxBytes(*p.BatchMaxBytes))
	}

	if o, err := producerCompressionOpt(p.BatchCompression); err != nil {
		return nil, err
	} else if o != nil {
		opts = append(opts, o)
	}

	if d, ok, err := parseDurationField(p.Linger); err != nil {
		return nil, fmt.Errorf("producer.linger: %w", err)
	} else if ok {
		opts = append(opts, kgo.ProducerLinger(d))
	}
	if d, ok, err := parseDurationField(p.ProduceRequestTimeout); err != nil {
		return nil, fmt.Errorf("producer.produce_request_timeout: %w", err)
	} else if ok {
		opts = append(opts, kgo.ProduceRequestTimeout(d))
	}
	if p.DisableIdempotentWrite != nil && *p.DisableIdempotentWrite {
		opts = append(opts, kgo.DisableIdempotentWrite())
	}

	return opts, nil
}

// FullClusterOpts returns franz-go options from cluster.client, cluster.producer, and cluster.consumer.
// Use it when a single [kgo.Client] both produces and consumes (for example a test pump).
func FullClusterOpts(env *config.Cluster) ([]kgo.Opt, error) {
	if env == nil {
		return nil, errors.New("cluster is nil")
	}
	b, err := ClientOpts(env, nil)
	if err != nil {
		return nil, err
	}
	po, err := ProducerClusterOpts(env)
	if err != nil {
		return nil, err
	}
	co, err := ConsumerClusterOpts(env)
	if err != nil {
		return nil, err
	}
	out := append(append(b, po...), co...)
	return out, nil
}

func requiredAcksFromString(s string) (kgo.Acks, error) {
	normalized, err := config.NormalizeRequiredAcks(s)
	if err != nil {
		return kgo.Acks{}, fmt.Errorf("producer.required_acks: %w", err)
	}
	switch normalized {
	case "all":
		return kgo.AllISRAcks(), nil
	case "none":
		return kgo.NoAck(), nil
	case "leader":
		return kgo.LeaderAck(), nil
	default:
		return kgo.Acks{}, fmt.Errorf("producer.required_acks: unsupported canonical value %q", normalized)
	}
}

func producerCompressionOpt(s string) (kgo.ProducerOpt, error) {
	normalized, err := config.NormalizeCompression(s)
	if err != nil {
		return nil, fmt.Errorf("producer.%w", err)
	}
	switch normalized {
	case "":
		return nil, nil
	case "none":
		return kgo.ProducerBatchCompression(kgo.NoCompression()), nil
	case "snappy":
		return kgo.ProducerBatchCompression(kgo.SnappyCompression()), nil
	case "zstd":
		return kgo.ProducerBatchCompression(kgo.ZstdCompression()), nil
	case "lz4":
		return kgo.ProducerBatchCompression(kgo.Lz4Compression()), nil
	case "gzip":
		return kgo.ProducerBatchCompression(kgo.GzipCompression()), nil
	default:
		return nil, fmt.Errorf("producer.batch_compression: unsupported %q", s)
	}
}

func tlsConfigFrom(t *config.TLS) (*tls.Config, error) {
	if t == nil || !t.Enabled {
		return nil, nil
	}
	cfg := &tls.Config{
		MinVersion: tls.VersionTLS12,
	}
	if t.CAFile != "" {
		ca, err := os.ReadFile(t.CAFile)
		if err != nil {
			return nil, fmt.Errorf("read tls ca_file: %w", err)
		}
		pool := x509.NewCertPool()
		if !pool.AppendCertsFromPEM(ca) {
			return nil, errors.New("tls ca_file: no valid PEM certificates")
		}
		cfg.RootCAs = pool
	}
	if t.CertFile != "" || t.KeyFile != "" {
		if t.CertFile == "" || t.KeyFile == "" {
			return nil, errors.New("tls: cert_file and key_file must both be set for client certificates")
		}
		cert, err := tls.LoadX509KeyPair(t.CertFile, t.KeyFile)
		if err != nil {
			return nil, fmt.Errorf("tls client cert: %w", err)
		}
		cfg.Certificates = []tls.Certificate{cert}
	}
	return cfg, nil
}

func saslMechanism(s *config.SASL) (sasl.Mechanism, error) {
	if s == nil {
		return nil, nil
	}
	mech := strings.ToLower(strings.TrimSpace(s.Mechanism))
	if mech == "" || mech == "none" {
		return nil, nil
	}
	switch mech {
	case "plain":
		return plain.Auth{User: s.Username, Pass: s.Password}.AsMechanism(), nil
	case "scram-sha-256":
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha256Mechanism(), nil
	case "scram-sha-512":
		return scram.Auth{User: s.Username, Pass: s.Password}.AsSha512Mechanism(), nil
	default:
		return nil, fmt.Errorf("sasl: unsupported mechanism %q", s.Mechanism)
	}
}
