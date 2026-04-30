package logging_test

import (
	"encoding/json"
	"io"
	"log/slog"
	"os"
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/observability/logging"
)

func TestSetup_JSONFieldsPresent(t *testing.T) {
	line := captureStream(t, func() {
		cleanup, err := logging.Setup(config.Logging{
			Level:  "info",
			Format: "json",
			Stream: "stdout",
			ExtraFields: map[string]string{
				"schema_version": "1.0",
				"service":        "bifrost",
				"env":            "test",
			},
		}, logging.WithSoftwareVersion("1.2.3-test"))
		if err != nil {
			t.Fatalf("Setup: %v", err)
		}
		defer cleanup()

		slog.Info("startup complete", "request_id", "r-1")
	})
	if line == "" {
		t.Fatal("expected log line")
	}
	var m map[string]any
	if err := json.Unmarshal([]byte(line), &m); err != nil {
		t.Fatalf("Unmarshal: %v", err)
	}

	for _, key := range []string{"software_version", "schema_version", "time", "level", "msg"} {
		if _, ok := m[key]; !ok {
			t.Fatalf("missing expected JSON field %q", key)
		}
	}
	if got, _ := m["software_version"].(string); got != "1.2.3-test" {
		t.Fatalf("software_version: got %q want %q", got, "1.2.3-test")
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
	line := captureStream(t, func() {
		cleanup, err := logging.Setup(config.Logging{
			Level:  "info",
			Format: "logfmt",
			Stream: "stdout",
			ExtraFields: map[string]string{
				"schema_version": "1.0",
				"service":        "bifrost",
			},
		}, logging.WithSoftwareVersion("1.2.3-test"))
		if err != nil {
			t.Fatalf("Setup: %v", err)
		}
		defer cleanup()

		slog.Info("bridge started")
	})
	if line == "" {
		t.Fatal("expected log line")
	}
	for _, want := range []string{"software_version=1.2.3-test", "schema_version=1.0", "service=bifrost", "msg=\"bridge started\""} {
		if !strings.Contains(line, want) {
			t.Fatalf("logfmt line missing %q: %s", want, line)
		}
	}
}

func TestSetup_rejectsFileStream(t *testing.T) {
	_, err := logging.Setup(config.Logging{
		Level:  "info",
		Format: "json",
		Stream: "file",
	})
	if err == nil {
		t.Fatal("expected unsupported file stream error")
	}
}

func captureStream(t *testing.T, fn func()) string {
	t.Helper()
	r, w, err := os.Pipe()
	if err != nil {
		t.Fatalf("Pipe: %v", err)
	}
	oldStdout := os.Stdout
	os.Stdout = w
	defer func() {
		os.Stdout = oldStdout
	}()

	fn()

	if err := w.Close(); err != nil {
		t.Fatalf("Close writer: %v", err)
	}
	data, err := io.ReadAll(r)
	if err != nil {
		t.Fatalf("ReadAll: %v", err)
	}
	if err := r.Close(); err != nil {
		t.Fatalf("Close reader: %v", err)
	}
	return strings.TrimSpace(string(data))
}
