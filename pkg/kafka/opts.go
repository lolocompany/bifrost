package kafka

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"

	"github.com/lolocompany/bifrost/pkg/config"
)

// ClientOpts returns franz-go options shared by producer and consumer clients for a cluster
// (brokers, TLS, SASL, and cluster.client).
func ClientOpts(env *config.Cluster) ([]kgo.Opt, error) {
	if env == nil {
		return nil, errors.New("cluster is nil")
	}
	var opts []kgo.Opt
	opts = append(opts, kgo.SeedBrokers(env.Brokers...))

	tlsCfg, err := tlsConfigFrom(&env.TLS)
	if err != nil {
		return nil, err
	}
	if tlsCfg != nil {
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
	if d, ok, err := parseDurationField(cl.DialTimeout); err != nil {
		return nil, fmt.Errorf("client.dial_timeout: %w", err)
	} else if ok {
		opts = append(opts, kgo.DialTimeout(d))
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

func parseDurationField(s string) (d time.Duration, set bool, err error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, false, nil
	}
	d, err = time.ParseDuration(s)
	if err != nil {
		return 0, false, err
	}
	return d, true, nil
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
	b, err := ClientOpts(env)
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
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all", "-1":
		return kgo.AllISRAcks(), nil
	case "none", "0":
		return kgo.NoAck(), nil
	case "leader", "1":
		return kgo.LeaderAck(), nil
	default:
		return kgo.Acks{}, fmt.Errorf("producer.required_acks: unsupported %q (use all, leader, none)", s)
	}
}

func producerCompressionOpt(s string) (kgo.ProducerOpt, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
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
		MinVersion:         tls.VersionTLS12,
		InsecureSkipVerify: t.InsecureSkipVerify,
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
