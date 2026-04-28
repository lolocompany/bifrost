package metrics

import (
	"fmt"
	"io"
	"net/http"
	"strings"
	"time"
)

func Fetch(endpoint string) (string, error) {
	resp, err := http.Get(endpoint)
	if err != nil {
		return "", err
	}
	defer func() {
		_ = resp.Body.Close()
	}()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("status %d", resp.StatusCode)
	}
	return string(body), nil
}

func WaitContains(endpoint string, timeout time.Duration, needles ...string) (string, error) {
	deadline := time.Now().Add(timeout)
	var last string
	for time.Now().Before(deadline) {
		body, err := Fetch(endpoint)
		if err == nil {
			last = body
			ok := true
			for _, n := range needles {
				if !strings.Contains(body, n) {
					ok = false
					break
				}
			}
			if ok {
				return body, nil
			}
		}
		time.Sleep(250 * time.Millisecond)
	}
	return last, fmt.Errorf("metrics endpoint missing expected content after %s", timeout)
}
