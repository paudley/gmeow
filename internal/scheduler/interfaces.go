// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
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
		limit int,
		refresh ProjectionRefreshFunc,
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
	ProjectObject(ctx context.Context, object filestore.ProjectionObject) error
}

type ProjectionRefreshFunc func(
	ctx context.Context,
	digests []contracts.ObjectDigest,
) error
