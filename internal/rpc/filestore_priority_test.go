// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rpc

import (
	"context"
	"sync"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

// recordingNotifier captures every NotifyObjectsChanged batch the filestore
// server's change-batcher delivers.
type recordingNotifier struct {
	mu       sync.Mutex
	requests []contracts.ObjectChangeRequest
}

func (n *recordingNotifier) NotifyObjectsChanged(
	_ context.Context,
	request contracts.ObjectChangeRequest,
) (contracts.SchedulerScanResponse, error) {
	n.mu.Lock()
	defer n.mu.Unlock()
	n.requests = append(n.requests, request)

	return contracts.SchedulerScanResponse{}, nil
}

func (n *recordingNotifier) snapshot() []contracts.ObjectChangeRequest {
	n.mu.Lock()
	defer n.mu.Unlock()

	return append([]contracts.ObjectChangeRequest(nil), n.requests...)
}

// TestChangeBatcherGroupsByPriorityClass proves the filestore change-batcher
// keeps producers on separate priority tracks: object-changed notices with
// distinct priority classes flush as separate NotifyObjectsChanged calls, each
// carrying its own class, rather than being flattened into one batch.
func TestChangeBatcherGroupsByPriorityClass(t *testing.T) {
	notifier := &recordingNotifier{}
	server := NewFilestoreServer(nil, WithObjectChangeNotifier(notifier))

	server.enqueueObjectChanged("digest-inbox-1", contracts.PriorityFreshIngest)
	server.enqueueObjectChanged("digest-backfill-1", contracts.PriorityBackground)
	server.enqueueObjectChanged("digest-inbox-2", contracts.PriorityFreshIngest)

	// Stop closes the change channel; the batcher does a final flush and exits.
	server.Stop()

	byClass := map[string][]contracts.ObjectDigest{}
	for _, request := range notifier.snapshot() {
		if request.ProjectionOnly {
			continue
		}
		byClass[request.PriorityClass] = append(
			byClass[request.PriorityClass],
			request.ObjectDigests...,
		)
	}

	if got := len(byClass[contracts.PriorityFreshIngest]); got != 2 {
		t.Fatalf("expected 2 fresh_ingest digests, got %d (%v)", got, byClass)
	}
	if got := len(byClass[contracts.PriorityBackground]); got != 1 {
		t.Fatalf("expected 1 background digest, got %d (%v)", got, byClass)
	}
}
