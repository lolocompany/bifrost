package bridge

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

func (o RunOptions) withDefaults() RunOptions {
	if o.Sleep == nil {
		o.Sleep = sleepContext
	}
	if o.Jitter == nil {
		o.Jitter = randomJitter
	}
	return o
}

func retryStage(
	ctx context.Context,
	log *slog.Logger,
	stage string,
	cfg RetryConfig,
	errorsSeen *atomic.Uint64,
	m MetricsReporter,
	id Identity,
	opts RunOptions,
	op func() error,
) error {
	attempt := 0
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := op(); err != nil {
			if ctxErr := ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			m.IncErrors(id, stage)
			errorsSeen.Add(1)
			attempt++
			base, jitter, sleepFor := retryDelay(attempt, cfg, opts.Jitter)
			log.Warn("bridge stage failed; retrying",
				"stage", stage,
				"attempt", attempt,
				"error_message", err.Error(),
				"base_backoff", base.String(),
				"jitter", jitter.String(),
				"sleep", sleepFor.String(),
			)
			if err := opts.Sleep(ctx, sleepFor); err != nil {
				if ctxErr := ctx.Err(); ctxErr != nil {
					return ctxErr
				}
				return err
			}
			continue
		}
		return nil
	}
}

func retryDelay(attempt int, cfg RetryConfig, jitterFn func(time.Duration) time.Duration) (time.Duration, time.Duration, time.Duration) {
	base := cfg.MinBackoff
	for i := 1; i < attempt; i++ {
		if base >= cfg.MaxBackoff {
			base = cfg.MaxBackoff
			break
		}
		if base > cfg.MaxBackoff/2 {
			base = cfg.MaxBackoff
			break
		}
		base *= 2
	}
	jitter := time.Duration(0)
	if cfg.Jitter > 0 && jitterFn != nil {
		jitter = jitterFn(cfg.Jitter)
		if jitter < 0 {
			jitter = 0
		}
		if jitter > cfg.Jitter {
			jitter = cfg.Jitter
		}
	}
	return base, jitter, base + jitter
}

func sleepContext(ctx context.Context, d time.Duration) error {
	if d <= 0 {
		return ctx.Err()
	}
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func randomJitter(max time.Duration) time.Duration {
	if max <= 0 {
		return 0
	}
	// #nosec G404 -- retry jitter only needs statistical spread, not cryptographic randomness.
	return time.Duration(rand.Int64N(int64(max) + 1))
}
