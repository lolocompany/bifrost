package config

import (
	"fmt"
	"strings"
	"time"
)

// ParseOptionalDuration parses a duration field and indicates whether a value was provided.
func ParseOptionalDuration(field, s string) (time.Duration, bool, error) {
	trimmed := strings.TrimSpace(s)
	if trimmed == "" {
		return 0, false, nil
	}
	d, err := time.ParseDuration(trimmed)
	if err != nil {
		return 0, false, fmt.Errorf("%s: parse duration: %w", field, err)
	}
	return d, true, nil
}

// NormalizeRequiredAcks converts configured producer.required_acks to canonical values.
func NormalizeRequiredAcks(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "", "all", "-1":
		return "all", nil
	case "none", "0":
		return "none", nil
	case "leader", "1":
		return "leader", nil
	default:
		return "", fmt.Errorf("required_acks: unsupported %q (use all, leader, none)", s)
	}
}

// NormalizeCompression converts producer.batch_compression to canonical names.
func NormalizeCompression(s string) (string, error) {
	switch strings.ToLower(strings.TrimSpace(s)) {
	case "":
		return "", nil
	case "none", "snappy", "zstd", "lz4", "gzip":
		return strings.ToLower(strings.TrimSpace(s)), nil
	default:
		return "", fmt.Errorf("batch_compression: unsupported %q (use snappy, zstd, lz4, gzip, none)", s)
	}
}
