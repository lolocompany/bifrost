package process

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"os"
	"os/exec"
	"path/filepath"
	"time"

	"github.com/lolocompany/bifrost/test/integration/testutil/artifacts"
)

type BifrostProcess struct {
	cmd      *exec.Cmd
	art      artifacts.TestArtifacts
	metrics  string
	rootPath string
}

func Start(ctx context.Context, art artifacts.TestArtifacts, metricsAddr string) (*BifrostProcess, error) {
	repoRoot, err := filepath.Abs("../..")
	if err != nil {
		return nil, fmt.Errorf("repo root: %w", err)
	}
	stdout, err := os.Create(art.Stdout)
	if err != nil {
		return nil, fmt.Errorf("stdout file: %w", err)
	}
	stderr, err := os.Create(art.Stderr)
	if err != nil {
		_ = stdout.Close()
		return nil, fmt.Errorf("stderr file: %w", err)
	}
	cmd := exec.CommandContext(ctx, "go", "run", "./cmd/bifrost", "--config", art.Config)
	cmd.Dir = repoRoot
	cmd.Stdout = stdout
	cmd.Stderr = stderr
	cmd.Env = append(os.Environ(), "BIFROST_CONFIG="+art.Config)
	if err := cmd.Start(); err != nil {
		_ = stdout.Close()
		_ = stderr.Close()
		return nil, fmt.Errorf("start bifrost: %w", err)
	}
	return &BifrostProcess{
		cmd:      cmd,
		art:      art,
		metrics:  metricsAddr,
		rootPath: repoRoot,
	}, nil
}

func (p *BifrostProcess) WaitReady(ctx context.Context) error {
	endpoint := "http://" + p.metrics + "/metrics"
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
			resp, err := http.Get(endpoint)
			if err != nil {
				continue
			}
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			_ = os.WriteFile(p.art.Metrics, body, 0o644)
			if resp.StatusCode == http.StatusOK {
				return nil
			}
		}
	}
}

func (p *BifrostProcess) Stop() error {
	if p.cmd == nil || p.cmd.Process == nil {
		return nil
	}
	if err := p.cmd.Process.Kill(); err != nil {
		return err
	}
	if err := p.cmd.Wait(); err != nil {
		var exitErr *exec.ExitError
		if errors.As(err, &exitErr) {
			return nil
		}
		return err
	}
	return nil
}
