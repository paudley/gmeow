// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package scheduler_test

import (
	"context"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
	"blackcat.ca/gmeow/internal/rpc"
	"blackcat.ca/gmeow/internal/scheduler"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestSpecsFromConfigAppliesKnownAnalyzerConstraints(t *testing.T) {
	specs := scheduler.SpecsFromConfig([]config.AnalyzerConfig{{
		Name:    "rfc822.headers",
		Version: "phase04-email-v2",
	}})
	if len(specs) != 1 {
		t.Fatalf("expected one spec, got %#v", specs)
	}
	if len(specs[0].MediaTypes) != 2 ||
		specs[0].MediaTypes[0] != "message/rfc822" ||
		specs[0].MediaTypes[1] != "text/rfc822-headers" {
		t.Fatalf("expected rfc822 media constraints, got %#v", specs[0])
	}
}

func TestScanEnqueuesMissingWork(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	digest := putTextObject(t, ctx, filestoreService, "hello")

	response, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		PriorityClass: contracts.PriorityBackground,
		RequestedBy:   "test",
		Reason:        "scan",
	})
	if err != nil {
		t.Fatal(err)
	}

	if response.Enqueued != 1 {
		t.Fatalf("expected one enqueued job for %s, got %#v", digest, response)
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

func TestScanTreatsPreCutoverAnalyzerVersionsAsStale(t *testing.T) {
	ctx := context.Background()
	specs := []contracts.AnalyzerSpec{
		{Name: "text.extract", Version: "phase04-email-v2"},
		{Name: "embedding.endpoint", Version: "phase04-email-v2"},
		{Name: "summary.model", Version: "phase04-email-v2"},
		{Name: "ner.spacy", Version: "python-email-v1"},
		{Name: "categories.sklearn", Version: "python-email-v1"},
	}
	filestoreService, schedulerService := startScheduler(t, ctx, specs)
	digest := putTextObject(t, ctx, filestoreService, "cutover")
	for _, stale := range []contracts.Annotation{
		staleAnalysisAnnotation(digest, "text.extract", "phase04"),
		staleAnalysisAnnotation(digest, "embedding.endpoint", "phase04"),
		staleAnalysisAnnotation(digest, "summary.model", "phase04"),
		staleAnalysisAnnotation(digest, "ner.spacy", "python-current"),
		staleAnalysisAnnotation(digest, "categories.sklearn", "python-current"),
	} {
		if err := filestoreService.Client.WriteAnnotation(ctx, stale); err != nil {
			t.Fatal(err)
		}
	}

	response, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{})
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != len(specs) || status.Pending != len(specs) {
		t.Fatalf(
			"expected all pre-cutover outputs to be rescheduled, response=%#v status=%#v",
			response,
			status,
		)
	}
}

func TestNotifyObjectsChangedProjectionOnlyDoesNotEnqueueAnalyzers(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	digest := putTextObject(t, ctx, filestoreService, "projection only")

	response, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion:  contracts.SchemaVersionPhase00,
			ObjectDigests:  []contracts.ObjectDigest{digest},
			RequestedBy:    "filestore",
			Reason:         "projection_refresh",
			ProjectionOnly: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if response.Enqueued != 1 || status.Pending != 0 {
		t.Fatalf("projection refresh must not enqueue analyzers, response=%#v status=%#v",
			response,
			status,
		)
	}
	duplicate, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion:  contracts.SchemaVersionPhase00,
			ObjectDigests:  []contracts.ObjectDigest{digest},
			RequestedBy:    "filestore",
			Reason:         "projection_refresh",
			ProjectionOnly: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if duplicate.Enqueued != 0 || duplicate.Skipped != 1 {
		t.Fatalf("duplicate projection refresh should be skipped: %#v", duplicate)
	}
	if _, err := schedulerService.Service.ProcessObjectChanges(ctx, 100); err != nil {
		t.Fatal(err)
	}
	status, err = schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 0 {
		t.Fatalf("projection-only object change must not enqueue analyzers: %#v", status)
	}
	afterProcess, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion:  contracts.SchemaVersionPhase00,
			ObjectDigests:  []contracts.ObjectDigest{digest},
			RequestedBy:    "filestore",
			Reason:         "projection_refresh",
			ProjectionOnly: true,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if afterProcess.Enqueued != 1 || afterProcess.Skipped != 0 {
		t.Fatalf("processed projection refresh should queue again: %#v", afterProcess)
	}
}

func TestNotifyObjectsChangedSchedulesOnlyMissingAnalyzerWork(t *testing.T) {
	ctx := context.Background()
	filestoreService, schedulerService := startScheduler(t, ctx, []contracts.AnalyzerSpec{{
		Name:       "text.extract",
		Version:    "1",
		MediaTypes: []string{"text/plain"},
	}})
	digest := putTextObject(t, ctx, filestoreService, "object changed")

	response, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ObjectDigests: []contracts.ObjectDigest{digest},
			RequestedBy:   "filestore",
			Reason:        "object_changed",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if response.Enqueued != 1 {
		t.Fatalf("expected object change to be queued: %#v", response)
	}
	if _, err := schedulerService.Service.ProcessObjectChanges(ctx, 100); err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 1 {
		t.Fatalf(
			"expected queued object change to enqueue missing analyzer work: %#v",
			status,
		)
	}
}

func TestNotifySkipsFullyAnnotatedViaLRU(t *testing.T) {
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
	digest := putTextObject(t, ctx, filestoreService, "fully done")

	if err := filestoreService.Client.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  spec.Name,
		AnalyzerVer:   spec.Version,
		SchemaVersion: contracts.SchemaVersionPhase00,
		Data:          map[string]any{"status": "complete"},
	}); err != nil {
		t.Fatal(err)
	}

	first, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ObjectDigests: []contracts.ObjectDigest{digest},
			RequestedBy:   "filestore",
			Reason:        "object_changed",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if first.Enqueued != 1 {
		t.Fatalf("expected fully annotated object change to be queued: %#v", first)
	}
	if _, err := schedulerService.Service.ProcessObjectChanges(ctx, 100); err != nil {
		t.Fatal(err)
	}

	second, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ObjectDigests: []contracts.ObjectDigest{digest},
			RequestedBy:   "filestore",
			Reason:        "object_changed",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if second.Enqueued != 1 {
		t.Fatalf("second notify should still queue the object change: %#v", second)
	}
	if _, err := schedulerService.Service.ProcessObjectChanges(ctx, 100); err != nil {
		t.Fatal(err)
	}
	status, err := schedulerService.Client.Status(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if status.Pending != 0 {
		t.Fatalf("LRU should prevent analyzer fanout for fully annotated object: %#v", status)
	}
}

func TestForceEvictsLRUAndEnqueues(t *testing.T) {
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
	digest := putTextObject(t, ctx, filestoreService, "force me")

	if err := filestoreService.Client.WriteAnnotation(ctx, contracts.Annotation{
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  spec.Name,
		AnalyzerVer:   spec.Version,
		SchemaVersion: contracts.SchemaVersionPhase00,
		Data:          map[string]any{"status": "complete"},
	}); err != nil {
		t.Fatal(err)
	}

	_, err := schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ObjectDigests: []contracts.ObjectDigest{digest},
			RequestedBy:   "filestore",
			Reason:        "object_changed",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	response, err := schedulerService.Client.Force(ctx, digest, nil, "operator", "trace")
	if err != nil {
		t.Fatal(err)
	}

	if response.Enqueued != 1 {
		t.Fatalf("force should enqueue despite LRU: %#v", response)
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

func TestSchedulerDoesNotWriteToFilestore(t *testing.T) {
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
	digest := putTextObject(t, ctx, filestoreService, "no writes")

	_, err := schedulerService.Client.Scan(ctx, contracts.SchedulerScanRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		RequestedBy:   "test",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = schedulerService.Client.NotifyObjectsChanged(
		ctx,
		contracts.ObjectChangeRequest{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ObjectDigests: []contracts.ObjectDigest{digest},
			RequestedBy:   "filestore",
			Reason:        "object_changed",
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	var schedulerAnnotationFound bool

	err = filestoreService.Store.WalkProjection(
		ctx,
		func(object filestore.ProjectionObject) error {
			for _, annotation := range object.Annotations {
				if annotation.Kind == "scheduler" {
					schedulerAnnotationFound = true
				}
			}

			return nil
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	if schedulerAnnotationFound {
		t.Fatal("scheduler must not write annotations to FILESTORE")
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

func staleAnalysisAnnotation(
	digest contracts.ObjectDigest,
	name string,
	version string,
) contracts.Annotation {
	return contracts.Annotation{
		ObjectDigest:  digest,
		Kind:          "analysis",
		AnalyzerName:  name,
		AnalyzerVer:   version,
		GeneratedAt:   time.Now().UTC(),
		SchemaVersion: contracts.SchemaVersionPhase00,
		Data:          map[string]any{"status": "complete"},
	}
}
