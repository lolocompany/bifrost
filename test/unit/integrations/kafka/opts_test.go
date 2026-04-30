package kafka_test

import (
	"testing"

	"github.com/lolocompany/bifrost/internal/config"
	"github.com/lolocompany/bifrost/internal/integrations/kafka"
)

func TestProducerClusterOpts_acceptsCanonicalAndAliasAcks(t *testing.T) {
	t.Parallel()
	cases := []string{"", "-1", "all", "0", "none", "1", "leader"}
	for _, acks := range cases {
		_, err := kafka.ProducerClusterOpts(&config.Cluster{
			Producer: config.ProducerSettings{
				RequiredAcks: acks,
			},
		})
		if err != nil {
			t.Fatalf("ProducerClusterOpts(required_acks=%q): %v", acks, err)
		}
	}
}

func TestProducerClusterOpts_rejectsUnsupportedCompression(t *testing.T) {
	t.Parallel()
	_, err := kafka.ProducerClusterOpts(&config.Cluster{
		Producer: config.ProducerSettings{
			RequiredAcks:     "all",
			BatchCompression: "brotli",
		},
	})
	if err == nil {
		t.Fatal("expected error for unsupported compression")
	}
}
