// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"bytes"
	"context"
	"fmt"
	"testing"
)

func newTestService() *Service {
	service := NewService(
		NewResolver(NewMemoryCache(), &stubEmbedder{dim: FullDim}),
		NewEntityIndex(FullDim, CoarseDim),
		"stub",
	)
	var counter int
	service.SetIDSource(
		func() string { counter++; return fmt.Sprintf("01ENTITY%04d", counter) },
	)

	return service
}

func claimInput(text string, isName bool) ClaimInput {
	return ClaimInput{Text: text, Hash: StatementHash(text), IsName: isName}
}

// TestServiceResolveChurnDoesNotCorruptIndex exercises the path that crashed the
// live run: heavy centroid churn (one entity repeatedly re-pooled as new claims
// arrive, plus many distinct entities), crossing the periodic-rebuild threshold.
// The no-Delete + periodic-rebuild index must never panic.
func TestServiceResolveChurnDoesNotCorruptIndex(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	// Repeatedly add new claims to one entity (forces many centroid updates) and
	// interleave many distinct new entities — well past rebuildEvery (256).
	for i := range 400 {
		grow := []ClaimInput{
			claimInput("name: grace hopper", true),
			claimInput(fmt.Sprintf("email: grace%d@navy.example", i), false),
		}
		if _, err := service.Resolve(ctx, grow, threshold, threshold); err != nil {
			t.Fatalf("grow resolve %d: %v", i, err)
		}
		distinct := []ClaimInput{
			claimInput(fmt.Sprintf("name: person %d unique", i), true),
			claimInput(fmt.Sprintf("email: p%d@distinct.example", i), false),
		}
		if _, err := service.Resolve(ctx, distinct, threshold, threshold); err != nil {
			t.Fatalf("distinct resolve %d: %v", i, err)
		}
	}
	if service.index.Len() < 100 {
		t.Fatalf("expected many distinct entities, got %d", service.index.Len())
	}
}

func TestServiceStateSnapshotRestoreKeepsIdempotency(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	claims := []ClaimInput{
		claimInput("name: grace hopper", true),
		claimInput("email: grace@navy.example", false),
	}
	first, err := service.Resolve(ctx, claims, threshold, threshold)
	if err != nil {
		t.Fatalf("initial resolve: %v", err)
	}
	if !first.IsNew {
		t.Fatalf("want a new entity, got %+v", first)
	}

	blob, err := service.SnapshotState()
	if err != nil {
		t.Fatalf("snapshot state: %v", err)
	}

	// A brand-new service (cold) loads the state and must resolve the same claims
	// to the SAME entity as a NOOP — cross-restart idempotency.
	restored := NewService(
		NewResolver(NewMemoryCache(), &stubEmbedder{dim: FullDim}),
		NewEntityIndex(FullDim, CoarseDim),
		"stub",
	)
	if err := restored.LoadState(blob); err != nil {
		t.Fatalf("load state: %v", err)
	}
	again, err := restored.Resolve(ctx, claims, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve after restore: %v", err)
	}
	if !again.IsNoop || again.Entity != first.Entity {
		t.Fatalf("post-restore resolve: want NOOP on %s, got %+v", first.Entity, again)
	}
}

func TestServiceObservationFrequencyTracksDistinctObservations(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	rooted := []ClaimInput{
		claimInput("email: shared@example.test", false),
		claimInput("phone: +15550100", false),
		claimInput("name-token: alpha", true),
	}
	first, err := service.Resolve(ctx, rooted, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve rooted: %v", err)
	}
	if !first.IsNew {
		t.Fatalf("rooted: want new entity, got %+v", first)
	}

	replay, err := service.Resolve(ctx, rooted, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve exact replay: %v", err)
	}
	if !replay.IsNoop || replay.Entity != first.Entity {
		t.Fatalf("exact replay: want NOOP on %s, got %+v", first.Entity, replay)
	}

	distinctNoop := []ClaimInput{
		claimInput("email: shared@example.test", false),
		claimInput("phone: +15550100", false),
	}
	second, err := service.Resolve(ctx, distinctNoop, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve distinct noop: %v", err)
	}
	if !second.IsNoop || second.Entity != first.Entity {
		t.Fatalf("distinct observation: want NOOP on %s, got %+v", first.Entity, second)
	}

	sharedEmail := StatementHash("email: shared@example.test")
	if got := service.ledger.observationCount(); got != 2 {
		t.Fatalf("observation count = %d, want 2", got)
	}
	if got := service.ledger.observationFreq(sharedEmail); got != 2 {
		t.Fatalf("shared email occurrence count = %d, want 2", got)
	}
}

func TestServiceStateSnapshotRestoreKeepsObservationFrequency(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	claims := []ClaimInput{
		claimInput("email: shared@example.test", false),
		claimInput("phone: +15550100", false),
	}
	if _, err := service.Resolve(ctx, claims, threshold, threshold); err != nil {
		t.Fatalf("resolve first observation: %v", err)
	}
	if _, err := service.Resolve(ctx, []ClaimInput{
		claimInput("email: shared@example.test", false),
		claimInput("phone: +15550101", false),
	}, threshold, threshold); err != nil {
		t.Fatalf("resolve second observation: %v", err)
	}

	blob, err := service.SnapshotState()
	if err != nil {
		t.Fatalf("snapshot state: %v", err)
	}

	restored := newTestService()
	if err := restored.LoadState(blob); err != nil {
		t.Fatalf("load state: %v", err)
	}

	sharedEmail := StatementHash("email: shared@example.test")
	if got := restored.ledger.observationCount(); got != 2 {
		t.Fatalf("restored observation count = %d, want 2", got)
	}
	if got := restored.ledger.observationFreq(sharedEmail); got != 2 {
		t.Fatalf("restored shared email occurrence count = %d, want 2", got)
	}
}

func TestServiceLoadPreviousStateSeedsObservationFrequency(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	claims := []ClaimInput{
		claimInput("email: legacy@example.test", false),
		claimInput("phone: +15550100", false),
	}
	if _, err := service.Resolve(ctx, claims, threshold, threshold); err != nil {
		t.Fatalf("resolve legacy observation: %v", err)
	}

	blob, err := service.SnapshotState()
	if err != nil {
		t.Fatalf("snapshot state: %v", err)
	}

	reader := bytes.NewReader(blob[len(stateMagic):])
	cacheBlob, err := readLenBytes(reader)
	if err != nil {
		t.Fatalf("read cache blob: %v", err)
	}
	indexBlob, err := readLenBytes(reader)
	if err != nil {
		t.Fatalf("read index blob: %v", err)
	}
	ledgerBlob, err := readLenBytes(reader)
	if err != nil {
		t.Fatalf("read ledger blob: %v", err)
	}
	memoBlob, err := readLenBytes(reader)
	if err != nil {
		t.Fatalf("read memo blob: %v", err)
	}

	legacyLedgerLen := len(ledgerBlob) - legacyOccurrenceStatsLen(service.ledger)
	var legacy bytes.Buffer
	legacy.WriteString(previousStateMagic)
	for _, part := range [][]byte{
		cacheBlob,
		indexBlob,
		ledgerBlob[:legacyLedgerLen],
		memoBlob,
	} {
		if err := writeLenBytes(&legacy, part); err != nil {
			t.Fatalf("write legacy state part: %v", err)
		}
	}

	restored := newTestService()
	if err := restored.LoadState(legacy.Bytes()); err != nil {
		t.Fatalf("load previous state: %v", err)
	}

	legacyEmail := StatementHash("email: legacy@example.test")
	if got := restored.ledger.observationCount(); got != 1 {
		t.Fatalf("seeded observation count = %d, want 1", got)
	}
	if got := restored.ledger.observationFreq(legacyEmail); got != 1 {
		t.Fatalf("seeded legacy email occurrence count = %d, want 1", got)
	}
}

func legacyOccurrenceStatsLen(ledger *entityLedger) int {
	size := 4 + 4 // observation count + occurrence map count
	for hash := range ledger.occurrences {
		size += 8 + len(hash) + 4
	}

	return size
}

func TestServiceResolveWorkedExample(t *testing.T) {
	ctx := context.Background()
	service := newTestService()
	const threshold = 0.5

	// 1) index.ttl: a rich rooted graph -> exactly one new entity. Names arrive as
	// role-free TOKENS post-extraction, so "Patrick Audley" is two name-token claims.
	indexTTL := []ClaimInput{
		claimInput("name-token: patrick", true),
		claimInput("name-token: audley", true),
		claimInput("email: paudley@blackcat.ca", false),
		claimInput("org: blackcat informatics", false),
	}
	first, err := service.Resolve(ctx, indexTTL, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve index.ttl: %v", err)
	}
	if !first.IsNew || first.IsNoop || len(first.NewClaimHashes) != 4 {
		t.Fatalf("index.ttl: want a new entity with 4 new claims, got %+v", first)
	}

	// 2) 2002 vCard: same person, one NEW email -> matches, delta is just the email.
	vcard2002 := []ClaimInput{
		claimInput("name-token: patrick", true),
		claimInput("name-token: audley", true),
		claimInput("email: paudley@blackcat.ca", false),
		claimInput("email: pat@new.example", false),
	}
	second, err := service.Resolve(ctx, vcard2002, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve 2002 vcard: %v", err)
	}
	if second.IsNew || second.IsNoop || second.Entity != first.Entity {
		t.Fatalf("2002 vcard: want match to %s, got %+v", first.Entity, second)
	}
	if len(second.NewClaimHashes) != 1 ||
		second.NewClaimHashes[0] != StatementHash("email: pat@new.example") {
		t.Fatalf(
			"2002 vcard delta=%+v, want exactly the new email hash",
			second.NewClaimHashes,
		)
	}

	// 3) 2004 vCard: nothing new -> NOOP.
	vcard2004 := []ClaimInput{
		claimInput("name-token: patrick", true),
		claimInput("name-token: audley", true),
		claimInput("email: paudley@blackcat.ca", false),
	}
	third, err := service.Resolve(ctx, vcard2004, threshold, threshold)
	if err != nil {
		t.Fatalf("resolve 2004 vcard: %v", err)
	}
	if !third.IsNoop || third.Entity != first.Entity {
		t.Fatalf("2004 vcard: want NOOP on %s, got %+v", first.Entity, third)
	}
}
