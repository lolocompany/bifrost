package config_test

import (
	"testing"

	"github.com/lolocompany/bifrost/internal/config"
)

func TestNormalizeRequiredAcks(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":       "all",
		"-1":     "all",
		"all":    "all",
		"0":      "none",
		"none":   "none",
		"1":      "leader",
		"leader": "leader",
	}
	for in, want := range cases {
		got, err := config.NormalizeRequiredAcks(in)
		if err != nil {
			t.Fatalf("NormalizeRequiredAcks(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("NormalizeRequiredAcks(%q) = %q, want %q", in, got, want)
		}
	}
	if _, err := config.NormalizeRequiredAcks("quorum"); err == nil {
		t.Fatal("expected error for unsupported required_acks")
	}
}

func TestNormalizeCompression(t *testing.T) {
	t.Parallel()
	cases := map[string]string{
		"":       "",
		"none":   "none",
		"snappy": "snappy",
		"zstd":   "zstd",
		"lz4":    "lz4",
		"gzip":   "gzip",
	}
	for in, want := range cases {
		got, err := config.NormalizeCompression(in)
		if err != nil {
			t.Fatalf("NormalizeCompression(%q): %v", in, err)
		}
		if got != want {
			t.Fatalf("NormalizeCompression(%q) = %q, want %q", in, got, want)
		}
	}
	if _, err := config.NormalizeCompression("brotli"); err == nil {
		t.Fatal("expected error for unsupported compression")
	}
}
