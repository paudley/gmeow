// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
)

type Runtime struct {
	concurrency int
	source      JobSource
	store       ObjectStore
	registry    *Registry
	now         func() time.Time
	breaker     *circuitBreaker
	parkBackoff time.Duration
}

type RuntimeOption func(*Runtime)

func NewRuntime(
	source JobSource,
	store ObjectStore,
	registry *Registry,
	options ...RuntimeOption,
) (*Runtime, error) {
	if source == nil {
		return nil, errors.New("analysis job source is required")
	}

	if store == nil {
		return nil, errors.New("analysis filestore is required")
	}

	if registry == nil {
		return nil, errors.New("analysis registry is required")
	}

	runtime := &Runtime{
		concurrency: 1,
		source:      source,
		store:       store,
		registry:    registry,
		now:         func() time.Time { return time.Now().UTC() },
		parkBackoff: 5 * time.Second,
	}
	for _, option := range options {
		option(runtime)
	}
	runtime.breaker = newCircuitBreaker(runtime.now)

	return runtime, nil
}

// WithParkBackoff sets how long the worker waits before re-checking a job whose
// analyzer circuit is open, bounding how often parked jobs are re-queued.
func WithParkBackoff(backoff time.Duration) RuntimeOption {
	return func(runtime *Runtime) {
		if backoff >= 0 {
			runtime.parkBackoff = backoff
		}
	}
}

func WithConcurrency(concurrency int) RuntimeOption {
	return func(runtime *Runtime) {
		if concurrency > 0 {
			runtime.concurrency = concurrency
		}
	}
}

func WithClock(now func() time.Time) RuntimeOption {
	return func(runtime *Runtime) {
		if now != nil {
			runtime.now = now
		}
	}
}

func (runtime *Runtime) Run(ctx context.Context) error {
	if runtime.concurrency <= 1 {
		return runtime.runLoop(ctx)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	errs := make(chan error, 1)
	var wait sync.WaitGroup
	for range runtime.concurrency {
		wait.Add(1)
		go func() {
			defer wait.Done()
			if err := runtime.runLoop(ctx); err != nil {
				select {
				case errs <- err:
					cancel()
				default:
				}
			}
		}()
	}

	done := make(chan struct{})
	go func() {
		wait.Wait()
		close(done)
	}()

	select {
	case err := <-errs:
		cancel()
		<-done

		return err
	case <-ctx.Done():
		<-done

		return ctx.Err()
	case <-done:
		return ctx.Err()
	}
}

func (runtime *Runtime) runLoop(ctx context.Context) error {
	for {
		receipt, err := runtime.source.Receive(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
				return ctx.Err()
			}

			return err
		}

		if err := runtime.Handle(ctx, receipt); err != nil {
			return err
		}
	}
}

func (runtime *Runtime) Handle(ctx context.Context, receipt JobReceipt) error {
	if receipt == nil {
		return errors.New("analysis job receipt is required")
	}

	job := receipt.Job()
	key := analyzerKey(job.Analyzer)

	// Circuit open and not yet time for a probe: park the job (it waits on the
	// work queue for the analyzer to recover) rather than running it and burning
	// retries against a known-down dependency.
	if !runtime.breaker.allow(key) {
		if err := runtime.sleepBeforePark(ctx); err != nil {
			return err
		}

		return runtime.park(ctx, receipt)
	}

	err := runtime.Process(ctx, job)
	if err != nil {
		// Bogus job: the target object no longer exists (e.g. removed or the
		// store was wiped). It can never succeed and is not an analyzer failure,
		// so drop it (ack) rather than retrying, parking, or tripping the
		// breaker — otherwise stale jobs churn forever.
		if errors.Is(err, os.ErrNotExist) {
			// Confirm the object itself is gone before dropping: Process can also
			// surface os.ErrNotExist from analyzer internals (e.g. a transiently
			// missing pack mid-repack), and those must not be ACK-dropped.
			if _, manifestErr := runtime.store.ReadManifest(ctx, job.ObjectDigest); errors.Is(
				manifestErr,
				os.ErrNotExist,
			) {
				observability.DefaultMetrics().AddCounter("gmeow_dropped_bogus_jobs", 1)
				if ackErr := receipt.Ack(ctx); ackErr != nil {
					return fmt.Errorf("ack bogus analysis job: %w", ackErr)
				}

				return nil
			}
		}

		runtime.breaker.recordFailure(key)
		// Park (wait for recovery) rather than retry toward a dead-letter when the
		// analyzer's backing service is unavailable, or when repeated failures
		// have tripped the circuit open. Keying the park decision on the
		// unavailability signal itself — not only on accumulated breaker state —
		// keeps it correct across worker restarts, which reset the in-memory
		// breaker: the very first availability failure parks. A failure that is
		// neither an availability signal nor enough to open the circuit is treated
		// as job-specific and follows the normal bounded-retry path.
		if errors.Is(err, ErrAnalyzerUnavailable) || runtime.breaker.isOpen(key) {
			return runtime.park(ctx, receipt)
		}

		retryErr := receipt.Retry(ctx, err)
		if retryErr != nil {
			return fmt.Errorf("route failed analysis job: %w", retryErr)
		}

		return nil
	}

	runtime.breaker.recordSuccess(key)
	if err := receipt.Ack(ctx); err != nil {
		return fmt.Errorf("ack analysis job: %w", err)
	}

	return nil
}

func (runtime *Runtime) park(ctx context.Context, receipt JobReceipt) error {
	if err := receipt.Park(ctx); err != nil {
		return fmt.Errorf("park analysis job: %w", err)
	}

	return nil
}

func (runtime *Runtime) sleepBeforePark(ctx context.Context) error {
	if runtime.parkBackoff <= 0 {
		return nil
	}

	timer := time.NewTimer(runtime.parkBackoff)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func (runtime *Runtime) Process(ctx context.Context, job contracts.AnalyzerJob) error {
	started := time.Now()
	if err := validateJob(job); err != nil {
		return err
	}

	analyzer, ok := runtime.registry.Analyzer(job.Analyzer)
	if !ok {
		return fmt.Errorf(
			"analyzer %s version %s is not registered; refusing lower-quality fallback",
			job.Analyzer.Name,
			job.Analyzer.Version,
		)
	}

	complete, err := runtime.store.HasAnalysisAnnotation(
		ctx,
		job.ObjectDigest,
		job.Analyzer.Name,
		job.Analyzer.Version,
	)
	if err != nil {
		observability.DefaultMetrics().AddCounter("gmeow_failed_analyzers", 1)
		return fmt.Errorf("check existing analysis annotation: %w", err)
	}
	if complete && !job.Forced {
		observability.DefaultMetrics().AddCounter("gmeow_skipped_analyzers", 1)
		return nil
	}

	annotation, err := analyzer.Analyze(ctx, runtime.store, job)
	if err != nil {
		observability.DefaultMetrics().AddCounter("gmeow_failed_analyzers", 1)
		return err
	}

	annotation = normalizeAnnotation(annotation, job, runtime.now())
	if err := runtime.store.WriteAnnotation(ctx, annotation); err != nil {
		observability.DefaultMetrics().AddCounter("gmeow_failed_analyzers", 1)
		return fmt.Errorf("write analysis annotation: %w", err)
	}
	observability.DefaultMetrics().
		ObserveDuration("gmeow_analysis_latency", time.Since(started))

	return nil
}

func validateJob(job contracts.AnalyzerJob) error {
	if job.SchemaVersion != 0 && job.SchemaVersion != contracts.SchemaVersionPhase00 {
		return fmt.Errorf("unsupported analysis job schema_version %d", job.SchemaVersion)
	}

	if strings.TrimSpace(string(job.ObjectDigest)) == "" {
		return errors.New("analysis job object_digest is required")
	}

	err := ValidateSpec(job.Analyzer)
	if err != nil {
		return err
	}

	return nil
}

func normalizeAnnotation(
	annotation contracts.Annotation,
	job contracts.AnalyzerJob,
	now time.Time,
) contracts.Annotation {
	annotation.SchemaVersion = contracts.SchemaVersionPhase00
	annotation.ObjectDigest = job.ObjectDigest
	annotation.Kind = "analysis"
	annotation.AnalyzerName = job.Analyzer.Name

	annotation.AnalyzerVer = job.Analyzer.Version
	if annotation.GeneratedAt.IsZero() {
		annotation.GeneratedAt = now
	}

	if annotation.Data == nil {
		annotation.Data = map[string]any{}
	}

	if _, ok := annotation.Data["status"]; !ok {
		annotation.Data["status"] = "complete"
	}

	annotation.Data["idempotency_key"] = job.IdempotencyKey

	return annotation
}

var _ Worker = (*Runtime)(nil)
