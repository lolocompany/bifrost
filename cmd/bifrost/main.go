// Command bifrost relays Kafka records between named clusters.
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lolocompany/bifrost/pkg/bridge"
	"github.com/lolocompany/bifrost/pkg/config"
	bifrostkafka "github.com/lolocompany/bifrost/pkg/kafka"
	"github.com/lolocompany/bifrost/pkg/logging"
	"github.com/lolocompany/bifrost/pkg/metrics"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/urfave/cli/v3"
	"golang.org/x/sync/errgroup"
)

func main() {
	defaultLogCleanup, err := logging.SetupDefaults()
	if err != nil {
		fmt.Fprintf(os.Stderr, "logging: %v\n", err)
		os.Exit(1)
	}
	defer defaultLogCleanup()

	root := &cli.Command{
		Name:    "bifrost",
		Usage:   "Relay messages between Kafka clusters",
		Version: getVersion(),
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "path to YAML config file",
				Sources: cli.EnvVars("BIFROST_CONFIG"),
				Value:   "bifrost.yaml",
			},
		},
		Action: run,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.Run(ctx, os.Args); err != nil {
		slog.Error("bifrost exited with error", "error_message", err)
		os.Exit(1)
	}
}

func run(ctx context.Context, c *cli.Command) error {
	path := c.String("config")
	cfg, err := config.Load(path)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logCleanup, err := logging.Setup(cfg.Logging)
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	defer logCleanup()

	reg := prometheus.NewRegistry()
	var reger prometheus.Registerer = reg
	if len(cfg.Metrics.ExtraLabels) > 0 {
		labels := prometheus.Labels{}
		for k, v := range cfg.Metrics.ExtraLabels {
			labels[k] = v
		}
		reger = prometheus.WrapRegistererWith(labels, reger)
	}
	m, brokerMetrics, err := metrics.New(reger, cfg.Metrics, cfg.Bridges)
	if err != nil {
		return fmt.Errorf("metrics: %w", err)
	}

	if cfg.Metrics.MetricsEnabled() {
		mux := http.NewServeMux()
		mux.Handle("/metrics", metrics.Handler(reg))
		srv := &http.Server{Handler: mux}

		ln, err := net.Listen("tcp", cfg.Metrics.ListenAddr)
		if err != nil {
			return fmt.Errorf("metrics listen: %w", err)
		}

		go func() {
			if err := srv.Serve(ln); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("metrics server exited", "error_message", err.Error())
			}
		}()

		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				slog.Error("metrics shutdown", "error_message", err.Error())
			}
		}()
	}

	producerClients := make(map[string]*kgo.Client)
	defer func() {
		for _, cl := range producerClients {
			cl.Close()
		}
	}()

	for _, br := range cfg.Bridges {
		clusterName := br.To.Cluster
		if _, ok := producerClients[clusterName]; ok {
			continue
		}
		toCluster := cfg.Clusters[clusterName]
		p, err := bifrostkafka.NewProducer(&toCluster, kafkaClientHooks(brokerMetrics, clusterName))
		if err != nil {
			return fmt.Errorf("producer %q: %w", clusterName, err)
		}
		pingCtx, cancelPing, err := bifrostkafka.WithPingTimeout(ctx, &toCluster)
		if err != nil {
			p.Close()
			return fmt.Errorf("producer %q ping: %w", clusterName, err)
		}
		if err := bifrostkafka.PingBroker(pingCtx, p); err != nil {
			cancelPing()
			p.Close()
			return fmt.Errorf("producer %q: broker unreachable: %w", clusterName, err)
		}
		cancelPing()
		producerClients[clusterName] = p
	}

	g, gctx := errgroup.WithContext(ctx)

	for _, br := range cfg.Bridges {
		g.Go(func() error {
			fromCluster := cfg.Clusters[br.From.Cluster]
			consumer, err := bifrostkafka.NewConsumerForBridge(
				&fromCluster,
				br.EffectiveConsumerGroup(),
				br.From.Topic,
				kafkaClientHooks(brokerMetrics, br.From.Cluster),
			)
			if err != nil {
				return fmt.Errorf("bridge %q consumer: %w", br.Name, err)
			}
			defer consumer.Close()

			pingCtx, cancelPing, err := bifrostkafka.WithPingTimeout(gctx, &fromCluster)
			if err != nil {
				return fmt.Errorf("bridge %q consumer ping: %w", br.Name, err)
			}
			if err := bifrostkafka.PingBroker(pingCtx, consumer); err != nil {
				cancelPing()
				return fmt.Errorf("bridge %q from cluster %q: broker unreachable: %w", br.Name, br.From.Cluster, err)
			}
			cancelPing()

			producer := producerClients[br.To.Cluster]
			slog.Info("bridge starting",
				"bridge", br.Name,
				"consumer_group", br.EffectiveConsumerGroup(),
				"from_cluster", br.From.Cluster,
				"to_cluster", br.To.Cluster,
			)
			if err := bridge.Run(gctx, bridge.IdentityFrom(br), consumer, producer, m); err != nil {
				if errors.Is(err, context.Canceled) {
					return nil
				}
				return fmt.Errorf("bridge %q: %w", br.Name, err)
			}
			return nil
		})
	}

	if err := g.Wait(); err != nil {
		return err
	}
	return nil
}

func kafkaClientHooks(broker *metrics.BrokerMetrics, cluster string) []kgo.Hook {
	if broker == nil {
		return nil
	}
	h := broker.HookFor(cluster)
	if h == nil {
		return nil
	}
	return []kgo.Hook{h}
}
