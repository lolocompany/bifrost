package config_test

import (
	"strings"
	"testing"

	"github.com/lolocompany/bifrost/internal/config"
)

func TestParse_metricsExtraLabelsRejectsStateLabel(t *testing.T) {
	const yamlDoc = `
clusters:
  east:
    brokers: ["127.0.0.1:9092"]
bridges:
  - name: east-loop
    from: { cluster: east, topic: in }
    to: { cluster: east, topic: out }
metrics:
  enabled: true
  listen_addr: ":9090"
  extra_labels:
    state: shadow
logging:
  level: info
  stream: stdout
`
	_, err := config.Parse([]byte(yamlDoc))
	if err == nil {
		t.Fatal("expected error for state extra label collision")
	}
	if !strings.Contains(err.Error(), "metrics.labels.extra") {
		t.Fatalf("error: %v", err)
	}
}
