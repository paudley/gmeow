// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

type Scheduler interface {
	Scan(
		ctx context.Context,
		request contracts.SchedulerScanRequest,
	) (contracts.SchedulerScanResponse, error)
	Enqueue(ctx context.Context, job contracts.AnalyzerJob) error
	Force(
		ctx context.Context,
		digest contracts.ObjectDigest,
		analyzerNames []string,
		requestedBy string,
		traceID string,
	) (contracts.SchedulerScanResponse, error)
	NotifyObjectsChanged(
		ctx context.Context,
		request contracts.ObjectChangeRequest,
	) (contracts.SchedulerScanResponse, error)
	Requeue(
		ctx context.Context,
		request contracts.RequeueRequest,
	) (contracts.RequeueResponse, error)
	ProcessFailures(
		ctx context.Context,
		request contracts.RequeueRequest,
	) (contracts.RequeueResponse, error)
	DeadLetters(
		ctx context.Context,
		request contracts.DeadLetterRequest,
	) (contracts.DeadLetterResponse, error)
	FailedJobs(
		ctx context.Context,
		request contracts.DeadLetterRequest,
	) (contracts.DeadLetterResponse, error)
	Status(ctx context.Context) (contracts.SchedulerStatus, error)
	Run(ctx context.Context) error
}

type Broker interface {
	Declare(ctx context.Context) error
	Publish(ctx context.Context, job contracts.AnalyzerJob) error
	PublishProjectionRefresh(ctx context.Context, digest contracts.ObjectDigest) error
	ProcessFailures(ctx context.Context, limit int) (int, error)
	ReconcilePending(
		ctx context.Context,
		limit int,
		isSatisfied func(contracts.AnalyzerJob) (bool, error),
	) (contracts.ReconcilePendingResponse, error)
	ProcessProjectionRefreshes(
		ctx context.Context,
		projector ProjectionRefresher,
		limit int,
	) (int, error)
	RouteFailure(ctx context.Context, job contracts.AnalyzerJob) error
	Status(ctx context.Context) (contracts.SchedulerStatus, error)
	ActiveJobKeys(ctx context.Context, limit int) (map[string]bool, error)
	FailedJobs(ctx context.Context, limit int) ([]contracts.AnalyzerJob, error)
	PendingJobs(ctx context.Context, limit int) ([]contracts.AnalyzerJob, error)
	DeadLetters(ctx context.Context, limit int) ([]contracts.AnalyzerJob, error)
	RequeueDeadLetters(ctx context.Context, limit int) (int, error)
	Close() error
}

type ProjectionRefresher interface {
	ProjectChanged(ctx context.Context, since time.Time) error
}
