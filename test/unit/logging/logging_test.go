package logging_test

import (
	"encoding/json"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/pkg/config"
	"github.com/lolocompany/bifrost/pkg/logging"
)

func TestSetup_JSONFieldsPresent(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "bifrost-log-*.jsonl")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close temp file: %v", err)
	}

	cleanup, err := logging.Setup(config.Logging{
		Level:    "info",
		Format:   "json",
		Stream:   "file",
		FilePath: tmp.Name(),
		ExtraFields: map[string]string{
			"schema_version": "1.0",
			"service":     "bifrost",
			"env":         "test",
		},
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer cleanup()

	slog.Info("startup complete", "request_id", "r-1")
	cleanup()

	data, err := os.ReadFile(tmp.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if line == "" {
		t.Fatal("expected log line")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, key := range []string{"schema_version", "time", "level", "msg"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("missing expected JSON field %q", key)
		}
	}
	if got, _ := m["schema_version"].(string); got != "1.0" {
		t.Fatalf("schema_version: got %q want %q", got, "1.0")
	}
	if got, _ := m["service"].(string); got != "bifrost" {
		t.Fatalf("service: got %q want %q", got, "bifrost")
	}
	if got, _ := m["env"].(string); got != "test" {
		t.Fatalf("env: got %q want %q", got, "test")
	}
}

func TestSetup_LogfmtKeepsExtraFields(t *testing.T) {
	tmp, err := os.CreateTemp(t.TempDir(), "bifrost-log-*.log")
	if err != nil {
		t.Fatalf("CreateTemp: %v", err)
	}
	if err := tmp.Close(); err != nil {
		t.Fatalf("Close temp file: %v", err)
	}
	cleanup, err := logging.Setup(config.Logging{
		Level:    "info",
		Format:   "logfmt",
		Stream:   "file",
		FilePath: tmp.Name(),
		ExtraFields: map[string]string{
			"schema_version": "1.0",
			"service":     "bifrost",
		},
	})
	if err != nil {
		t.Fatalf("Setup: %v", err)
	}
	defer cleanup()

	slog.Info("bridge started")
	cleanup()

	data, err := os.ReadFile(tmp.Name())
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	line := strings.TrimSpace(string(data))
	if line == "" {
		t.Fatal("expected log line")
	}
	for _, want := range []string{"schema_version=1.0", "service=bifrost", "msg=\"bridge started\""} {
		if !strings.Contains(line, want) {
			t.Fatalf("logfmt line missing %q: %s", want, line)
		}
	}
}

