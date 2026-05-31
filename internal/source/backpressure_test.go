// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package source

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/testsupport"
)

// fakePressure is a scripted PressureReporter for exercising the backfill gate.
type fakePressure struct {
	mu        sync.Mutex
	pressured bool
	err       error
	calls     int
}

func (p *fakePressure) Pressured(context.Context) (bool, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++

	return p.pressured, p.err
}

func (p *fakePressure) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()

	return p.calls
}

func TestAwaitPressureReliefNilReporterReturnsImmediately(t *testing.T) {
	service := &Service{}
	if err := service.awaitPressureRelief(context.Background()); err != nil {
		t.Fatalf("nil reporter must not block or error: %v", err)
	}
}

func TestAwaitPressureReliefProceedsWhenNotPressured(t *testing.T) {
	reporter := &fakePressure{pressured: false}
	service := &Service{pressure: reporter}

	if err := service.awaitPressureRelief(context.Background()); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if reporter.callCount() != 1 {
		t.Fatalf("expected one pressure check, got %d", reporter.callCount())
	}
}

func TestAwaitPressureReliefFailsOpenOnError(t *testing.T) {
	reporter := &fakePressure{pressured: true, err: errors.New("scheduler unreachable")}
	service := &Service{pressure: reporter}

	// A pressure-check error must not wedge ingest: the page proceeds.
	if err := service.awaitPressureRelief(context.Background()); err != nil {
		t.Fatalf("pressure-check error must fail open, got: %v", err)
	}
}

func TestAwaitPressureReliefHonoursCancellationWhilePressured(t *testing.T) {
	reporter := &fakePressure{pressured: true}
	service := &Service{pressure: reporter}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if err := service.awaitPressureRelief(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("expected context.Canceled while paused, got: %v", err)
	}
}

// countingPullAdapter yields one object then reports completion, recording how
// many pages were pulled so a test can assert the backfill gate ran first.
type countingPullAdapter struct {
	pages int
}

func (*countingPullAdapter) Name() string { return "primary" }

func (*countingPullAdapter) Kind() string { return "gmail" }

func (*countingPullAdapter) Capabilities() []string { return []string{CapabilityBackfill} }

func (adapter *countingPullAdapter) Pull(
	_ context.Context,
	_ IngestService,
	request PullRequest,
) ([]IngestObject, contracts.SourceCursor, error) {
	adapter.pages++
	cursor := cloneCursor(request.Cursor)
	cursor["completed"] = true

	return []IngestObject{
			{
				Reader:      strings.NewReader("hello"),
				SourceKind:  "gmail",
				SourceName:  "primary",
				ExternalID:  "message-1",
				ExternalVer: "v1",
				Facets:      []contracts.Facet{{Kind: "file"}},
			},
		},
		contracts.SourceCursor{SourceKind: "gmail", SourceName: "primary", Cursor: cursor},
		nil
}

// TestRunBackfillConsultsPressureForBackgroundPriority proves the gate is wired
// into the page loop for low-priority background backfill: the reporter is
// consulted before any page is pulled.
func TestRunBackfillConsultsPressureForBackgroundPriority(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()

	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	reporter := &fakePressure{pressured: false}
	service.SetPressureReporter(reporter)

	adapter := &countingPullAdapter{}
	request := BackfillRequest{PriorityClass: contracts.PriorityBackground, PageSize: 10}
	if _, err := service.RunBackfill(ctx, adapter, request); err != nil {
		t.Fatal(err)
	}

	if reporter.callCount() < 1 {
		t.Fatalf(
			"background backfill must consult pressure before pulling, calls=%d",
			reporter.callCount(),
		)
	}
	if adapter.pages < 1 {
		t.Fatalf("expected at least one page pulled, got %d", adapter.pages)
	}
}

// TestRunBackfillDoesNotThrottleHighPriority proves inbox/search/import-class
// runs are never gated on backpressure: the reporter is not consulted even when
// it would report pressure.
func TestRunBackfillDoesNotThrottleHighPriority(t *testing.T) {
	ctx := context.Background()
	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()

	service, err := NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	reporter := &fakePressure{pressured: true}
	service.SetPressureReporter(reporter)

	adapter := &countingPullAdapter{}
	request := BackfillRequest{PriorityClass: contracts.PriorityFreshIngest, PageSize: 10}
	if _, err := service.RunBackfill(ctx, adapter, request); err != nil {
		t.Fatal(err)
	}

	if reporter.callCount() != 0 {
		t.Fatalf(
			"high-priority run must not consult pressure, calls=%d",
			reporter.callCount(),
		)
	}
	if adapter.pages < 1 {
		t.Fatalf("expected at least one page pulled, got %d", adapter.pages)
	}
}
