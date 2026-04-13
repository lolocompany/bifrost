// Package logging configures the process-wide slog logger from config.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"
	"sort"

	"github.com/lolocompany/bifrost/pkg/config"
)

// SetupOption configures logging.Setup (functional options).
type SetupOption func(*setupConfig)

type setupConfig struct {
	softwareVersion string
}

// WithSoftwareVersion adds a software_version attribute to every log record (JSON key
// "software_version"). Callers should pass the application release or build version string.
func WithSoftwareVersion(v string) SetupOption {
	return func(c *setupConfig) {
		c.softwareVersion = v
	}
}

// SetupDefaults configures slog with the same defaults as config.applyDefaults for the logging
// section (info level, JSON to stdout). Use before loading a config file so startup errors use
// the same handler shape as a minimal valid config; call Setup(cfg.Logging) after Load to apply
// the file’s logging settings.
func SetupDefaults(opts ...SetupOption) (func(), error) {
	return Setup(config.Logging{
		Level:  "info",
		Format: "json",
		Stream: "stdout",
	}, opts...)
}

// Setup configures slog from cfg and returns a cleanup function (e.g. close log file).
func Setup(cfg config.Logging, setupOpts ...SetupOption) (func(), error) {
	level, err := parseLevel(cfg.LevelKey())
	if err != nil {
		return nil, err
	}

	var w io.Writer
	cleanup := func() {}
	switch cfg.StreamKey() {
	case "stdout":
		w = os.Stdout
	case "stderr":
		w = os.Stderr
	case "file":
		f, err := os.OpenFile(cfg.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o600)
		if err != nil {
			return nil, fmt.Errorf("open log file: %w", err)
		}
		cleanup = func() {
			if err := f.Close(); err != nil {
				// Stderr: log file may be unusable after a failed close.
				slog.Error("close log file", "error_message", err)
			}
		}
		w = f
	default:
		return nil, fmt.Errorf("unsupported log stream %q", cfg.Stream)
	}

	handlerOpts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch cfg.FormatKey() {
	case "json":
		h = slog.NewJSONHandler(w, handlerOpts)
	case "logfmt":
		// slog text handler emits key=value lines (logfmt-style).
		h = slog.NewTextHandler(w, handlerOpts)
	default:
		return nil, fmt.Errorf("unsupported log format %q", cfg.Format)
	}
	o := &setupConfig{softwareVersion: "unknown"}
	for _, opt := range setupOpts {
		opt(o)
	}
	logger := slog.New(h).With(append([]any{"software_version", o.softwareVersion}, extraArgs(cfg.ExtraFields)...)...)
	slog.SetDefault(logger)
	return cleanup, nil
}

func extraArgs(fields map[string]string) []any {
	if len(fields) == 0 {
		return nil
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	out := make([]any, 0, len(keys)*2)
	for _, k := range keys {
		out = append(out, k, fields[k])
	}
	return out
}

func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "trace":
		// slog has no trace; DEBUG is the closest severity level.
		return slog.LevelDebug, nil
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "fatal":
		// slog has no fatal; ERROR is the closest severity level.
		return slog.LevelError, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unsupported log level %q", s)
	}
}
