// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"context"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/config"
	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/source"
	"blackcat.ca/gmeow/internal/testsupport"
)

func TestConfiguredSourceWorkRunsInboxRefreshWhileBackfillIsActive(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	filestoreService := testsupport.StartFilestoreGRPC(t, ctx)
	defer filestoreService.Close()
	service, err := source.NewService(filestoreService.Client)
	if err != nil {
		t.Fatal(err)
	}
	adapter := &blockingPullAdapter{calls: make(chan string, 4)}

	errs := make(chan error, 1)
	go func() {
		errs <- runConfiguredSourceWork(ctx, service, adapter, config.SourceConfig{
			Backfill: config.SourceBackfillConfig{
				Enabled: true,
				Query:   "slow-backfill",
			},
			InboxRefresh: config.SourceInboxRefreshConfig{
				Enabled:  true,
				Query:    "in:inbox newer_than:30d",
				Interval: "1h",
			},
		})
	}()

	seen := map[string]bool{}
	deadline := time.After(2 * time.Second)
	for !seen["backfill"] || !seen["inbox_refresh"] {
		select {
		case call := <-adapter.calls:
			seen[call] = true
		case err := <-errs:
			t.Fatalf("configured source work exited early: %v", err)
		case <-deadline:
			t.Fatalf("expected concurrent backfill and inbox refresh calls, got %#v", seen)
		}
	}

	cancel()
	select {
	case err := <-errs:
		if err != nil {
			t.Fatalf("configured source work returned cancellation error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("configured source work did not stop after context cancellation")
	}
}

type blockingPullAdapter struct {
	calls chan string
}

func (*blockingPullAdapter) Name() string {
	return "primary"
}

func (*blockingPullAdapter) Kind() string {
	return "gmail"
}

func (*blockingPullAdapter) Capabilities() []string {
	return []string{source.CapabilityBackfill}
}

func (adapter *blockingPullAdapter) Pull(
	ctx context.Context,
	_ source.IngestService,
	request source.PullRequest,
) ([]source.IngestObject, contracts.SourceCursor, error) {
	query, _ := request.Cursor["query"].(string)
	if query == "slow-backfill" {
		adapter.calls <- "backfill"
		<-ctx.Done()

		return nil, contracts.SourceCursor{}, ctx.Err()
	}

	adapter.calls <- "inbox_refresh"
	cursor := cloneMap(request.Cursor)
	cursor["completed"] = true

	return nil, contracts.SourceCursor{
		SourceKind: "gmail",
		SourceName: "primary",
		Cursor:     cursor,
	}, nil
}

func cloneMap(values map[string]any) map[string]any {
	cloned := make(map[string]any, len(values))
	for key, value := range values {
		cloned[key] = value
	}

	return cloned
}
