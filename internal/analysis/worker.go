// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type Runtime struct {
	source   JobSource
	store    ObjectStore
	registry *Registry
	now      func() time.Time
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
		source:   source,
		store:    store,
		registry: registry,
		now:      func() time.Time { return time.Now().UTC() },
	}
	for _, option := range options {
		option(runtime)
	}
	return runtime, nil
}

func WithClock(now func() time.Time) RuntimeOption {
	return func(runtime *Runtime) {
		if now != nil {
			runtime.now = now
		}
	}
}

func (runtime *Runtime) Run(ctx context.Context) error {
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
	err := runtime.Process(ctx, job)
	if err != nil {
		if retryErr := receipt.Retry(ctx, err); retryErr != nil {
			return fmt.Errorf("route failed analysis job: %w", retryErr)
		}
		return nil
	}
	if err := receipt.Ack(ctx); err != nil {
		return fmt.Errorf("ack analysis job: %w", err)
	}
	return nil
}

func (runtime *Runtime) Process(ctx context.Context, job contracts.AnalyzerJob) error {
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
	annotation, err := analyzer.Analyze(ctx, runtime.store, job)
	if err != nil {
		return err
	}
	annotation = normalizeAnnotation(annotation, job, runtime.now())
	if err := runtime.store.WriteAnnotation(ctx, annotation); err != nil {
		return fmt.Errorf("write analysis annotation: %w", err)
	}
	return nil
}

func validateJob(job contracts.AnalyzerJob) error {
	if job.SchemaVersion != 0 && job.SchemaVersion != contracts.SchemaVersionPhase00 {
		return fmt.Errorf("unsupported analysis job schema_version %d", job.SchemaVersion)
	}
	if strings.TrimSpace(string(job.ObjectDigest)) == "" {
		return errors.New("analysis job object_digest is required")
	}
	if err := ValidateSpec(job.Analyzer); err != nil {
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
