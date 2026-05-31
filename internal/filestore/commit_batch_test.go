// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"io"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

// TestStoreChunkBatchedDedupsWithinObject locks the in-batch dedup invariant: an
// identical chunk appearing twice in one object's commit batch is appended to a
// pack and indexed exactly once, and the committed chunk still round-trips.
func TestStoreChunkBatchedDedupsWithinObject(t *testing.T) {
	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	batch, err := store.newCommitBatch()
	if err != nil {
		t.Fatal(err)
	}
	defer batch.close()

	chunk := bytes.Repeat([]byte("dedup-within-object "), 256)

	first, err := store.storeChunkBatched(ctx, batch, chunk)
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.storeChunkBatched(ctx, batch, chunk)
	if err != nil {
		t.Fatal(err)
	}

	if first != second {
		t.Fatalf("identical chunk yielded different hashes: %s vs %s", first, second)
	}
	if len(batch.seen) != 1 {
		t.Fatalf("expected 1 unique chunk in batch, got %d", len(batch.seen))
	}
	if len(batch.touchedPacks) != 1 {
		t.Fatalf("expected the chunk appended to 1 pack, got %d", len(batch.touchedPacks))
	}

	if err := batch.commit(); err != nil {
		t.Fatal(err)
	}

	got, err := store.readChunk(first)
	if err != nil {
		t.Fatalf("read committed chunk: %v", err)
	}
	if !bytes.Equal(got, chunk) {
		t.Fatal("committed chunk did not round-trip")
	}
}

// TestBatchedPutRoundTripAndCrossPutDedup confirms a batched Put commits an
// object that reads back byte-for-byte, and that re-putting identical content is
// idempotent — it reuses the already-committed chunks (cross-Put dedup via the
// committed chunk index) rather than appending duplicates.
func TestBatchedPutRoundTripAndCrossPutDedup(t *testing.T) {
	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	content := bytes.Repeat([]byte("the quick brown fox jumps over 0123456789 "), 4000)

	first, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(content),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	reader, err := store.Open(ctx, first)
	if err != nil {
		t.Fatal(err)
	}
	got, err := io.ReadAll(reader)
	_ = reader.Close()
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("batched Put did not round-trip")
	}

	chunksAfterFirst := countMetaPrefix(t, store, "c/")

	second, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(content),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if first != second {
		t.Fatalf("identical content produced different digests: %s vs %s", first, second)
	}

	chunksAfterSecond := countMetaPrefix(t, store, "c/")
	if chunksAfterSecond != chunksAfterFirst {
		t.Fatalf(
			"re-put of identical content created new chunks: before=%d after=%d",
			chunksAfterFirst, chunksAfterSecond,
		)
	}
}

func countMetaPrefix(t *testing.T, store *FilesystemStore, prefix string) int {
	t.Helper()
	count := 0
	if err := store.metaIterPrefix(prefix, func(string, []byte) error {
		count++

		return nil
	}); err != nil {
		t.Fatalf("iterate %q: %v", prefix, err)
	}

	return count
}
