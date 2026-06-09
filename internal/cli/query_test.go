// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"reflect"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/filestore"
)

func TestQueueChangedProjectionRefreshesBatchesProjectionOnlySchedulerNotices(
	t *testing.T,
) {
	ctx := context.Background()
	since := time.Date(2026, 6, 9, 2, 2, 0, 0, time.UTC)
	fresh := since.Add(time.Minute)
	stale := since.Add(-time.Minute)
	walker := &fakeChangedProjectionWalker{
		objects: []filestore.ProjectionObject{
			{Digest: "a", Manifest: contracts.Manifest{UpdatedAt: since}},
			{Digest: "b", Manifest: contracts.Manifest{UpdatedAt: since}},
			{Digest: "c", Manifest: contracts.Manifest{UpdatedAt: since}},
			{Digest: "d", Manifest: contracts.Manifest{UpdatedAt: since}},
			{
				Digest:   "e",
				Manifest: contracts.Manifest{UpdatedAt: stale},
				Annotations: []contracts.Annotation{{
					GeneratedAt: since,
				}},
			},
		},
	}
	timestamps := fakeProjectionTimestampReader{
		projectedAt: map[contracts.ObjectDigest]time.Time{
			"a": fresh,
			"b": stale,
			"d": fresh,
		},
	}
	notifier := &recordingProjectionRefreshNotifier{}

	report, err := queueChangedProjectionRefreshes(
		ctx,
		walker,
		timestamps,
		notifier,
		since,
		2,
	)
	if err != nil {
		t.Fatal(err)
	}

	if report.scanned != 5 ||
		report.enqueued != 3 ||
		report.skipped != 2 ||
		report.failed != 0 {
		t.Fatalf("unexpected report: %#v", report)
	}
	if !walker.since.Equal(since) {
		t.Fatalf("since = %s, want %s", walker.since, since)
	}

	got := make([][]contracts.ObjectDigest, 0, len(notifier.requests))
	for _, request := range notifier.requests {
		if request.RequestedBy != "query.project-changed" ||
			request.Reason != "projection_refresh" ||
			!request.ProjectionOnly {
			t.Fatalf("unexpected scheduler request: %#v", request)
		}
		got = append(got, request.ObjectDigests)
	}

	want := [][]contracts.ObjectDigest{
		{"b"},
		{"c"},
		{"e"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("scheduler batches = %#v, want %#v", got, want)
	}
}

type fakeChangedProjectionWalker struct {
	objects []filestore.ProjectionObject
	since   time.Time
}

func (walker *fakeChangedProjectionWalker) WalkChangedProjection(
	ctx context.Context,
	since time.Time,
	fn filestore.ProjectionFunc,
) error {
	walker.since = since
	for _, object := range walker.objects {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := fn(object); err != nil {
			return err
		}
	}

	return nil
}

type fakeProjectionTimestampReader struct {
	projectedAt map[contracts.ObjectDigest]time.Time
}

func (reader fakeProjectionTimestampReader) ProjectedAt(
	context.Context,
	[]contracts.ObjectDigest,
) (map[contracts.ObjectDigest]time.Time, error) {
	return reader.projectedAt, nil
}

type recordingProjectionRefreshNotifier struct {
	requests []contracts.ObjectChangeRequest
	skipped  int
}

func (notifier *recordingProjectionRefreshNotifier) NotifyObjectsChanged(
	_ context.Context,
	request contracts.ObjectChangeRequest,
) (contracts.SchedulerScanResponse, error) {
	notifier.requests = append(notifier.requests, request)

	return contracts.SchedulerScanResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Enqueued:      len(request.ObjectDigests),
		Skipped:       notifier.skipped,
	}, nil
}
