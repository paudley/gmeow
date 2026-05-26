// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler

import (
	"context"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestScanEnqueuesMissingWorkIdempotently(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	broker := NewMemoryBroker()
	service := newTestService(t, store, broker, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	first, err := service.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "test",
		Reason:        "scan",
	})
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "test",
		Reason:        "scan",
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs := broker.Jobs()
	if first.Enqueued != 1 || second.Enqueued != 0 || len(jobs) != 1 {
		t.Fatalf(
			"expected one effective job across repeated scans, first=%#v second=%#v jobs=%#v",
			first,
			second,
			jobs,
		)
	}
	if jobs[0].ObjectDigest != digest || jobs[0].IdempotencyKey == "" {
		t.Fatalf("unexpected job: %#v", jobs[0])
	}
}

func TestScanDetectsStaleAndFailedOutputs(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	staleDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("stale"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
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
	failedDigest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("failed"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
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
	broker := NewMemoryBroker()
	service := newTestService(t, store, broker, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "2",
		MediaTypes: []string{"text/plain"},
	}})
	response, err := service.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 2 {
		t.Fatalf("expected stale and failed jobs, got %#v", response)
	}
	reasons := map[string]bool{}
	for _, job := range broker.Jobs() {
		reasons[job.Reason] = true
	}
	if !reasons["stale"] || !reasons["repair"] {
		t.Fatalf("expected stale and repair reasons, got %#v", reasons)
	}
}

func TestInteractivePriorityOutranksBackground(t *testing.T) {
	store := filestore.NewFilesystemStore(t.TempDir())
	broker := NewMemoryBroker()
	service := newTestService(t, store, broker, []contracts.AnalyzerSpec{{
		Name:    "noop",
		Version: "1",
	}})
	interactive := contracts.AnalyzerJob{
		SchemaVersion: contracts.SchemaVersionPhase00,
		ObjectDigest:  contracts.ObjectDigest(strings.Repeat("a", 64)),
		Analyzer:      service.specs[0],
		PriorityClass: contracts.PriorityInteractive,
	}
	background := interactive
	background.PriorityClass = contracts.PriorityBackground
	background.ObjectDigest = contracts.ObjectDigest(strings.Repeat("b", 64))
	if err := service.Enqueue(context.Background(), interactive); err != nil {
		t.Fatal(err)
	}
	if err := service.Enqueue(context.Background(), background); err != nil {
		t.Fatal(err)
	}
	jobs := broker.Jobs()
	values := map[string]int{}
	for _, job := range jobs {
		values[job.PriorityClass] = job.Priority
	}
	if values[contracts.PriorityInteractive] <= values[contracts.PriorityBackground] {
		t.Fatalf("interactive priority did not outrank background: %#v", values)
	}
}

func TestInteractiveScanReprioritizesScheduledBackgroundWork(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	broker := NewMemoryBroker()
	service := newTestService(t, store, broker, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	background, err := service.Scan(ctx, contracts.SchedulerScanRequest{
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "scheduler",
	})
	if err != nil {
		t.Fatal(err)
	}
	interactive, err := service.Scan(ctx, contracts.SchedulerScanRequest{
		PriorityClass: contracts.PriorityInteractive,
		RequestedBy:   "interface",
	})
	if err != nil {
		t.Fatal(err)
	}
	jobs := broker.Jobs()
	if background.Enqueued != 1 ||
		interactive.Enqueued != 1 ||
		len(jobs) != 1 ||
		jobs[0].ObjectDigest != digest ||
		jobs[0].PriorityClass != contracts.PriorityInteractive {
		t.Fatalf(
			"expected one reprioritized interactive job, background=%#v interactive=%#v jobs=%#v",
			background,
			interactive,
			jobs,
		)
	}
}

func TestForceUsesSchedulerMarkersForIdempotency(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	broker := NewMemoryBroker()
	service := newTestService(t, store, broker, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	first, err := service.Force(ctx, digest, nil, "operator", "trace")
	if err != nil {
		t.Fatal(err)
	}
	second, err := service.Force(ctx, digest, nil, "operator", "trace")
	if err != nil {
		t.Fatal(err)
	}
	if first.Enqueued != 1 || second.Enqueued != 0 || len(broker.Jobs()) != 1 {
		t.Fatalf("forced reanalysis was not idempotent: first=%#v second=%#v jobs=%#v",
			first,
			second,
			broker.Jobs(),
		)
	}
}

func TestDeadLetterRequeueMovesJobsBackToPending(t *testing.T) {
	broker := NewMemoryBroker()
	job := contracts.AnalyzerJob{
		SchemaVersion:  contracts.SchemaVersionPhase00,
		JobID:          "job",
		IdempotencyKey: "job",
		ObjectDigest:   "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		Analyzer:       contracts.AnalyzerSpec{Name: "noop", Version: "1"},
	}
	broker.AddDeadLetter(job)
	service := newTestService(
		t,
		filestore.NewFilesystemStore(t.TempDir()),
		broker,
		[]contracts.AnalyzerSpec{job.Analyzer},
	)
	response, err := service.Requeue(
		context.Background(),
		contracts.RequeueRequest{Limit: 10},
	)
	if err != nil {
		t.Fatal(err)
	}
	status, err := service.Status(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if response.Requeued != 1 || status.Pending != 1 || status.DeadLetter != 0 {
		t.Fatalf("unexpected requeue state: response=%#v status=%#v", response, status)
	}
}

func TestStalePublishingMarkerIsRetried(t *testing.T) {
	ctx := context.Background()
	store := filestore.NewFilesystemStore(t.TempDir())
	digest, err := store.Put(ctx, filestore.PutRequest{
		Reader:    strings.NewReader("hello"),
		MediaType: "text/plain",
		Facets:    []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	broker := NewMemoryBroker()
	service := newTestService(t, store, broker, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	job := contracts.AnalyzerJob{
		ObjectDigest:  digest,
		Analyzer:      service.specs[0],
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "scheduler",
		Reason:        "missing",
		CreatedAt:     time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC),
	}
	job.IdempotencyKey = IdempotencyKey(job)
	if err := store.WriteAnnotation(ctx, contracts.Annotation{
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
	response, err := service.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 1 {
		t.Fatalf("expected stale publishing marker to be retried, got %#v", response)
	}
}

func newTestService(
	t *testing.T,
	store filestore.Store,
	broker Broker,
	specs []contracts.AnalyzerSpec,
) *Service {
	t.Helper()
	service, err := NewService(store, broker, specs, Config{
		RetryBackoff: 30 * time.Second,
		Priorities: config.SchedulerPriority{
			Interactive: 100,
			Forced:      90,
			FreshIngest: 70,
			Repair:      50,
			Background:  10,
		},
	}, WithClock(func() time.Time {
		return time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	}))
	if err != nil {
		t.Fatal(err)
	}
	return service
}
