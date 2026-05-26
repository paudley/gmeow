// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"sync"

	"blackcat.ca/gmeow/internal/contracts"
)

type MemoryBroker struct {
	mutex             sync.Mutex
	jobs              map[string]contracts.AnalyzerJob
	failed            []contracts.AnalyzerJob
	deadLetters       []contracts.AnalyzerJob
	projectionRefresh []contracts.ObjectDigest
	retryLimit        int
}

func NewMemoryBroker() *MemoryBroker {
	return &MemoryBroker{jobs: map[string]contracts.AnalyzerJob{}, retryLimit: 3}
}

func (broker *MemoryBroker) Declare(context.Context) error {
	return nil
}

func (broker *MemoryBroker) Publish(
	ctx context.Context,
	job contracts.AnalyzerJob,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	broker.jobs[job.IdempotencyKey] = job
	return nil
}

func (broker *MemoryBroker) PublishProjectionRefresh(
	ctx context.Context,
	digest contracts.ObjectDigest,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	broker.projectionRefresh = append(broker.projectionRefresh, digest)
	return nil
}

func (broker *MemoryBroker) RouteFailure(
	ctx context.Context,
	job contracts.AnalyzerJob,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	delete(broker.jobs, job.IdempotencyKey)
	job.Attempt++
	if job.Attempt > 3 {
		broker.deadLetters = append(broker.deadLetters, job)
		return nil
	}
	broker.jobs[job.IdempotencyKey] = job
	return nil
}

func (broker *MemoryBroker) ProcessFailures(
	ctx context.Context,
	limit int,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	if limit <= 0 || limit > len(broker.failed) {
		limit = len(broker.failed)
	}
	for _, job := range broker.failed[:limit] {
		job.Attempt++
		if job.Attempt > broker.retryLimit {
			broker.deadLetters = append(broker.deadLetters, job)
			continue
		}
		broker.jobs[job.IdempotencyKey] = job
	}
	broker.failed = append([]contracts.AnalyzerJob(nil), broker.failed[limit:]...)
	return limit, nil
}

func (broker *MemoryBroker) Status(context.Context) (contracts.SchedulerStatus, error) {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	return contracts.SchedulerStatus{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Pending:       len(broker.jobs),
		Failed:        len(broker.failed),
		DeadLetter:    len(broker.deadLetters),
	}, nil
}

func (broker *MemoryBroker) DeadLetters(
	_ context.Context,
	limit int,
) ([]contracts.AnalyzerJob, error) {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	if limit <= 0 || limit > len(broker.deadLetters) {
		limit = len(broker.deadLetters)
	}
	return append([]contracts.AnalyzerJob(nil), broker.deadLetters[:limit]...), nil
}

func (broker *MemoryBroker) RequeueDeadLetters(
	ctx context.Context,
	limit int,
) (int, error) {
	if err := ctx.Err(); err != nil {
		return 0, err
	}
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	if limit <= 0 || limit > len(broker.deadLetters) {
		limit = len(broker.deadLetters)
	}
	for _, job := range broker.deadLetters[:limit] {
		broker.jobs[job.IdempotencyKey] = job
	}
	broker.deadLetters = append(
		[]contracts.AnalyzerJob(nil),
		broker.deadLetters[limit:]...)
	return limit, nil
}

func (broker *MemoryBroker) Close() error {
	return nil
}

func (broker *MemoryBroker) Jobs() []contracts.AnalyzerJob {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	jobs := make([]contracts.AnalyzerJob, 0, len(broker.jobs))
	for _, job := range broker.jobs {
		jobs = append(jobs, job)
	}
	return jobs
}

func (broker *MemoryBroker) AddDeadLetter(job contracts.AnalyzerJob) {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	broker.deadLetters = append(broker.deadLetters, job)
}

func (broker *MemoryBroker) AddFailed(job contracts.AnalyzerJob) {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	broker.failed = append(broker.failed, job)
}

func (broker *MemoryBroker) ProjectionRefreshes() []contracts.ObjectDigest {
	broker.mutex.Lock()
	defer broker.mutex.Unlock()
	return append([]contracts.ObjectDigest(nil), broker.projectionRefresh...)
}

var _ Broker = (*MemoryBroker)(nil)
