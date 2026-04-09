// Command bifrost relays Kafka records between named clusters.
package main

import (
	"context"
	"errors"
	"fmt"
	"log"
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
	root := &cli.Command{
		Name:  "bifrost",
		Usage: "Relay messages between Kafka clusters",
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "path to YAML config file",
				Value:   "bifrost.yaml",
			},
		},
		Action: run,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.Run(ctx, os.Args); err != nil {
		log.Fatal(err)
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
	m, brokerProm, err := metrics.New(reg, cfg.Metrics, cfg.Bridges)
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
				slog.Error("metrics server exited", "err", err)
			}
		}()

		defer func() {
			shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			if err := srv.Shutdown(shutdownCtx); err != nil {
				slog.Error("metrics shutdown", "err", err)
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
		p, err := bifrostkafka.NewProducer(&toCluster, kafkaHooks(brokerProm, clusterName))
		if err != nil {
			return fmt.Errorf("producer %q: %w", clusterName, err)
		}
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
				kafkaHooks(brokerProm, br.From.Cluster),
			)
			if err != nil {
				return fmt.Errorf("bridge %q consumer: %w", br.Name, err)
			}
			defer consumer.Close()

			producer := producerClients[br.To.Cluster]
			slog.Info("bridge starting",
				"bridge", br.Name,
				"consumer_group", br.EffectiveConsumerGroup(),
				"from_cluster", br.From.Cluster,
				"to_cluster", br.To.Cluster,
			)
			if err := bridge.Run(gctx, metrics.BridgeIdentityFrom(br), consumer, producer, m); err != nil {
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

func kafkaHooks(bp *metrics.BrokerProm, cluster string) []kgo.Hook {
	if bp == nil {
		return nil
	}
	h := bp.HookFor(cluster)
	if h == nil {
		return nil
	}
	return []kgo.Hook{h}
}
