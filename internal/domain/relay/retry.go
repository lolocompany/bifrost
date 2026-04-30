package relay

import (
	"context"
	"log/slog"
	"math/rand/v2"
	"sync/atomic"
	"time"
)

type retryContext struct {
	ctx        context.Context
	log        *slog.Logger
	stage      string
	cfg        RetryConfig
	errorsSeen *atomic.Uint64
	metrics    Metrics
	id         Identity
	opts       Options
}

func (o Options) withDefaults() Options {
	if o.Sleep == nil {
		o.Sleep = sleepContext
	}
	if o.Jitter == nil {
		o.Jitter = randomJitter
	}
	return o
}

func retryStage(rc retryContext, op func() error) error {
	attempt := 0
	for {
		if err := rc.ctx.Err(); err != nil {
			return err
		}
		err := runRetryAttempt(rc.stage, rc.metrics, rc.id, op)
		if err != nil {
			if ctxErr := rc.ctx.Err(); ctxErr != nil {
				return ctxErr
			}
			nextAttempt, retryErr := handleRetryFailure(rc, attempt, err)
			if retryErr != nil {
				return retryErr
			}
			attempt = nextAttempt
			continue
		}
		return nil
	}
}

func runRetryAttempt(stage string, m Metrics, id Identity, op func() error) error {
	opStart := time.Now()
	err := op()
	if stage == StageProduce {
		m.AddProducerSeconds(id, RelayStateBusy, time.Since(opStart).Seconds())
	}
	return err
}

func handleRetryFailure(rc retryContext, attempt int, opErr error) (int, error) {
	rc.metrics.IncErrors(rc.id, rc.stage)
	rc.errorsSeen.Add(1)
	attempt++
	if rc.cfg.MaxAttempts > 0 && attempt >= rc.cfg.MaxAttempts {
		return attempt, opErr
	}
	base, jitter, sleepFor := retryDelay(attempt, rc.cfg, rc.opts.Jitter)
	rc.log.Warn("bridge stage failed; retrying",
		"stage", rc.stage,
		"attempt", attempt,
		"error_message", opErr.Error(),
		"base_backoff", base.String(),
		"jitter", jitter.String(),
		"sleep", sleepFor.String(),
	)
	if rc.stage == StageProduce {
		rc.metrics.AddProducerSeconds(rc.id, RelayStateIdle, sleepFor.Seconds())
	}
	if err := rc.opts.Sleep(rc.ctx, sleepFor); err != nil {
		if ctxErr := rc.ctx.Err(); ctxErr != nil {
			return attempt, ctxErr
		}
		return attempt, err
	}
	return attempt, nil
}

func retryDelay(attempt int, cfg RetryConfig, jitterFn func(time.Duration) time.Duration) (time.Duration, time.Duration, time.Duration) {
	return ComputeRetryDelay(attempt, cfg.MinBackoff, cfg.MaxBackoff, cfg.Jitter, jitterFn)
}

// ComputeRetryDelay returns exponential backoff and bounded jitter for retry loops.
func ComputeRetryDelay(
	attempt int,
	minBackoff time.Duration,
	maxBackoff time.Duration,
	maxJitter time.Duration,
	jitterFn func(time.Duration) time.Duration,
) (time.Duration, time.Duration, time.Duration) {
	base := minBackoff
	for i := 1; i < attempt; i++ {
		if base >= maxBackoff {
			base = maxBackoff
			break
		}
		if base > maxBackoff/2 {
			base = maxBackoff
			break
		}
		base *= 2
	}
	jitter := time.Duration(0)
	if maxJitter > 0 && jitterFn != nil {
		jitter = jitterFn(maxJitter)
		if jitter < 0 {
			jitter = 0
		}
		if jitter > maxJitter {
			jitter = maxJitter
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

func randomJitter(maxDuration time.Duration) time.Duration {
	if maxDuration <= 0 {
		return 0
	}
	// #nosec G404 -- retry jitter only needs statistical spread, not cryptographic randomness.
	return time.Duration(rand.Int64N(int64(maxDuration) + 1))
}
