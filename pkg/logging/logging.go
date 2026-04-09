// Package logging configures the process-wide slog logger from config.
package logging

import (
	"fmt"
	"io"
	"log/slog"
	"os"

	"github.com/lolocompany/bifrost/pkg/config"
)

// Setup configures slog from cfg and returns a cleanup function (e.g. close log file).
func Setup(cfg config.Logging) (func(), error) {
	level, err := parseLevel(cfg.LevelKey())
	if err != nil {
		return nil, err
	}

	var w io.Writer
	var cleanup func() = func() {}
	switch cfg.StreamKey() {
	case "stdout":
		w = os.Stdout
	case "stderr":
		w = os.Stderr
	case "file":
		f, err := os.OpenFile(cfg.FilePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0o644)
		if err != nil {
			return nil, fmt.Errorf("open log file: %w", err)
		}
		cleanup = func() { _ = f.Close() }
		w = f
	default:
		return nil, fmt.Errorf("unsupported log stream %q", cfg.Stream)
	}

	opts := &slog.HandlerOptions{Level: level}
	var h slog.Handler
	switch cfg.FormatKey() {
	case "json":
		h = slog.NewJSONHandler(w, opts)
	case "logfmt":
		// slog's text handler emits key=value lines (logfmt-style structured logs).
		h = slog.NewTextHandler(w, opts)
	default:
		return nil, fmt.Errorf("unsupported log format %q", cfg.Format)
	}

	slog.SetDefault(slog.New(h))
	return cleanup, nil
}

func parseLevel(s string) (slog.Level, error) {
	switch s {
	case "debug":
		return slog.LevelDebug, nil
	case "info":
		return slog.LevelInfo, nil
	case "warn":
		return slog.LevelWarn, nil
	case "error":
		return slog.LevelError, nil
	default:
		return slog.LevelInfo, fmt.Errorf("unsupported log level %q", s)
	}
}
