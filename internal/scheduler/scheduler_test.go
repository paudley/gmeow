// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/scheduler"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestScanEnqueuesMissingWorkIdempotently(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	digest := putTextObject(t, ctx, filestoreService, "hello")

	first, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "test",
		Reason:        "scan",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "test",
		Reason:        "scan",
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Enqueued != 1 || second.Enqueued != 0 || status.Pending != 1 {
		t.Fatalf(
			"expected one effective job across repeated scans for %s, first=%#v second=%#v status=%#v",
			digest,
			first,
			second,
			status,
		)
	}
}

func TestScanDetectsStaleAndFailedOutputs(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "2",
		MediaTypes: []string{"text/plain"},
	}})
	staleDigest := putTextObject(t, ctx, filestoreService, "stale")
	if err := filestoreService.Client.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest:  staleDigest,
		Kind:          "analysis",
		AnalyzerName:  "text.extract",
		AnalyzerVer:   "1",
		GeneratedAt:   time.Now().UTC(),
		SchemaVersion: contracts.SchemaVersionPhase00,
		Data:          map[string]any{"status": "complete"},
	}); err != nil {
		t.Fatal(err)
	}
	failedDigest := putTextObject(t, ctx, filestoreService, "failed")
	if err := filestoreService.Client.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest:  failedDigest,
		Kind:          "analysis",
		AnalyzerName:  "text.extract",
		AnalyzerVer:   "2",
		GeneratedAt:   time.Now().UTC(),
		SchemaVersion: contracts.SchemaVersionPhase00,
		Data:          map[string]any{"status": "failed"},
	}); err != nil {
		t.Fatal(err)
	}

	response, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 2 || status.Pending != 2 {
		t.Fatalf("expected stale and failed jobs, response=%#v status=%#v", response, status)
	}
}

func TestInteractiveScanAddsHigherPriorityWorkForScheduledBackgroundWork(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	putTextObject(t, ctx, filestoreService, "hello")

	background, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	interactive, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		PriorityClass: contracts.PriorityInteractive,
		RequestedBy:   "interface",
	})
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if background.Enqueued != 1 || interactive.Enqueued != 1 || status.Pending != 2 {
		t.Fatalf(
			"expected background work plus higher-priority interactive work, background=%#v interactive=%#v status=%#v",
			background,
			interactive,
			status,
		)
	}
}

func TestForceUsesSchedulerMarkersForIdempotency(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	digest := putTextObject(t, ctx, filestoreService, "hello")

	first, err := schedulerService.Client.Force(ctx, digest, nil, "operator", "trace")
	if err != nil {
		t.Fatal(err)
	}
	second, err := schedulerService.Client.Force(ctx, digest, nil, "operator", "trace")
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if first.Enqueued != 1 || second.Enqueued != 0 || status.Pending != 1 {
		t.Fatalf("forced reanalysis was not idempotent: first=%#v second=%#v status=%#v",
			first,
			second,
			status,
		)
	}
}

func TestDeadLetterRequeueMovesJobsBackToPending(t *testing.T) {
	ctx := context.Background()
	_, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:    "noop",
		Version: "1",
	}})
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "job",
		IdempotencyKey: "job",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
		Attempt:        1,
	}
	if err := schedulerService.Broker.RouteFailure(ctx, job); err != nil {
		t.Fatal(err)
	}
	response, err := schedulerService.Client.Requeue(
		ctx,
		contracts.RequeueRequest{Limit: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Requeued != 1 || status.Pending != 1 || status.DeadLetter != 0 {
		t.Fatalf("unexpected requeue state: response=%#v status=%#v", response, status)
	}
}

func TestStalePublishingMarkerIsRetried(t *testing.T) {
	ctx := context.Background()
	spec := contracts.AnalyzerSpec{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}
	filestoreService, schedulerService := startScheduler(
		t,
		ctx,
		[]contracts.AnalyzerSpec{spec},
	)
	digest := putTextObject(t, ctx, filestoreService, "hello")
	job := contracts.AnalyzerJob{
		ObjectDigest:  digest,
		Analyzer:      spec,
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "scheduler",
		Reason:        "missing",
		CreatedAt:     time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
	}
	job.IdempotencyKey = scheduler.IdempotencyKey(job)
	if err := filestoreService.Client.WriteAnnotation(ctx, contracts.Annotation{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  digest,
		Kind:          "scheduler",
		GeneratedAt:   time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC),
		Data: map[string]any{"scheduled_jobs": map[string]any{
			job.IdempotencyKey: map[string]any{
				"status": "publishing",
				"updated_at": time.Date(2026, 5, 25, 11, 0, 0, 0, time.UTC).
					Format(time.RFC3339Nano),
			},
		}},
	}); err != nil {
		t.Fatal(err)
	}

	response, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 1 || status.Pending != 1 {
		t.Fatalf("expected stale publishing marker to be retried, response=%#v status=%#v",
			response,
			status,
		)
	}
}

func startScheduler(
	t *testing.T,
	ctx context.Context,
	specs []contracts.AnalyzerSpec,
) (*testsupport.FilestoreService, *testsupport.SchedulerService) {
	t.Helper()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	t.Cleanup(filestoreService.Close)
	schedulerService := testsupport.StartSchedulerGRPC(
		t,
		ctx,
		filestoreService.Store,
		specs,
	)
	t.Cleanup(schedulerService.Close)

	return filestoreService, schedulerService
}

func putTextObject(
	t *testing.T,
	ctx context.Context,
	filestoreService *testsupport.FilestoreService,
	body string,
) contracts.ObjectDigest {
	t.Helper()
	digest, err := filestoreService.Client.Put(ctx, rpc.PutRequest{
		Reader:    strings.NewReader(body),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	return digest
}
