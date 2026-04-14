// Command bifrost relays Kafka records between named clusters.
package main

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/lolocompany/bifrost/cmd/bifrost/version"
	"github.com/lolocompany/bifrost/pkg/bifrost"
	"github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/logging"

	"github.com/urfave/cli/v3"
)

func main() {
	vi := version.Info()
	defaultLogCleanup, err := logging.SetupDefaults(logging.WithSoftwareVersion(vi.Version))
	if err != nil {
		fmt.Fprintf(os.Stderr, "logging: %v\n", err)
		os.Exit(1)
	}
	defer defaultLogCleanup()

	cli.VersionPrinter = printVersion

	root := &cli.Command{
		Name:    "bifrost",
		Usage:   "Relay messages between Kafka clusters",
		Version: vi.Version,
		Flags: []cli.Flag{
			&cli.StringFlag{
				Name:    "config",
				Aliases: []string{"c"},
				Usage:   "path to YAML config file",
				Sources: cli.EnvVars("BIFROST_CONFIG"),
				Value:   "bifrost.yaml",
			},
		},
		Action: runCLI,
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if err := root.Run(ctx, os.Args); err != nil {
		slog.Error("bifrost exited with error", "error_message", err)
		os.Exit(1)
	}
}

func runCLI(ctx context.Context, c *cli.Command) error {
	path := c.String("config")
	cfg, err := config.Load(path)
	if err != nil {
		return fmt.Errorf("load config: %w", err)
	}

	logCleanup, err := logging.Setup(cfg.Logging, logging.WithSoftwareVersion(version.Info().Version))
	if err != nil {
		return fmt.Errorf("logging: %w", err)
	}
	defer logCleanup()

	slog.Info("config loaded", "config_path", filepath.Clean(path))

	return bifrost.Run(ctx, *cfg)
}

func printVersion(cmd *cli.Command) {
	info := version.Info()
	_, err := fmt.Fprintf(cmd.Root().Writer, "%s version %s\nrevision %s\nbuild_time %s\n",
		cmd.Name, info.Version, info.Revision, info.BuildTime)
	if err != nil {
		slog.Error("write version", "error_message", err.Error())
	}
}
