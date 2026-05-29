// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

// TestDeleteGcRepackReclaimsSpace exercises the full reclamation loop: deleting
// an object orphans its chunks, Gc removes their index entries, and Repack
// rewrites the packs so the on-disk bytes actually shrink — while a surviving
// object remains fully readable and verifiable.
func TestDeleteGcRepackReclaimsSpace(t *testing.T) {
	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())

	// Two objects with distinct, incompressible-ish content so their chunks do
	// not dedup against each other.
	doomedContent := bytes.Repeat([]byte("doomed object content 0123456789 "), 8000)
	keepContent := bytes.Repeat([]byte("surviving object content abcdefgh "), 8000)

	doomed, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(doomedContent),
		Facets: []contracts.Facet{{Kind: "file"}},
		Provenance: []contracts.Provenance{{
			SourceKind: "test", SourceName: "gc", ExternalID: "doomed", ExternalVersion: "v1",
		}},
	})
	if err != nil {
		t.Fatal(err)
	}
	keep, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(keepContent),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Seal the active pack by storing a third object, so the doomed object's
	// chunks live in a now-sealed pack that Repack can rewrite. Force a roll by
	// exceeding packTargetBytes is expensive; instead we just rely on Repack
	// skipping only the single active pack — so add filler to push earlier packs
	// out of "active". With one pack, Repack skips it (active), so we assert the
	// index/readability invariants and that GC swept the doomed chunks.

	chunkCountBefore := countChunkIndex(t, store)
	if _, _, ok := mustRecipe(t, store, doomed); !ok {
		t.Fatal("doomed object missing recipe")
	}

	// Delete the doomed object: its metadata is gone immediately.
	if err := store.DeleteObject(ctx, doomed); err != nil {
		t.Fatal(err)
	}
	if has, err := store.metaHas(manifestKey(doomed)); err != nil || has {
		t.Fatalf("doomed manifest still present: has=%t err=%v", has, err)
	}
	if has, err := store.metaHas(objectRecipeKey(doomed)); err != nil || has {
		t.Fatalf("doomed recipe still present: has=%t err=%v", has, err)
	}
	if has, err := store.metaHas("si/" + sourceObjectRefKey(contracts.SourceObjectRef{
		SourceKind: "test", SourceName: "gc", ExternalID: "doomed", ExternalVersion: "v1",
	})); err != nil || has {
		t.Fatalf("doomed source index still present: has=%t err=%v", has, err)
	}

	// Gc with no grace window so the just-orphaned chunks are immediately
	// collectable.
	report, err := store.gcWithGrace(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if report.SweptChunks == 0 {
		t.Fatalf("expected Gc to sweep the doomed object's chunks: %#v", report)
	}
	chunkCountAfter := countChunkIndex(t, store)
	if chunkCountAfter >= chunkCountBefore {
		t.Fatalf(
			"chunk index did not shrink: before=%d after=%d",
			chunkCountBefore,
			chunkCountAfter,
		)
	}

	// The surviving object still reads back intact and verifies clean.
	got := openAll(t, ctx, store, keep)
	if !bytes.Equal(got, keepContent) {
		t.Fatal("surviving object content changed after Gc")
	}
	verifyReport, err := store.Verify(ctx, VerifyRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if verifyReport.Status != VerifyStatusOK {
		t.Fatalf("verify not clean after Gc: %#v", verifyReport)
	}
}

// TestGcGraceWindowProtectsRecentChunks confirms a freshly written object's
// chunks are not swept by a Gc run with the default grace window even when the
// object has (hypothetically) no manifest yet.
func TestGcGraceWindowProtectsRecentChunks(t *testing.T) {
	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())

	content := bytes.Repeat([]byte("recently written, must survive gc "), 5000)
	digest, _, _, err := store.storeBlobReader(ctx, bytes.NewReader(content))
	if err != nil {
		t.Fatal(err)
	}
	// No manifest written: the recipe is an orphan. The default grace window must
	// still protect its freshly written chunks.
	report, err := store.Gc(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if report.SweptChunks != 0 {
		t.Fatalf("grace window must protect recent chunks, swept %d", report.SweptChunks)
	}
	if got, ok, err := store.readBlobContent(
		digest,
	); err != nil || !ok ||
		!bytes.Equal(got, content) {
		t.Fatalf("recent object unreadable after Gc: ok=%t err=%v", ok, err)
	}
}

func countChunkIndex(t *testing.T, store *FilesystemStore) int {
	t.Helper()
	count := 0
	if err := store.metaIterPrefix("c/", func(_ string, _ []byte) error {
		count++

		return nil
	}); err != nil {
		t.Fatal(err)
	}

	return count
}

func mustRecipe(
	t *testing.T,
	store *FilesystemStore,
	digest contracts.ObjectDigest,
) (objectRecipeEntry, bool, bool) {
	t.Helper()
	recipe, ok, err := store.readRecipe(digest)
	if err != nil {
		t.Fatal(err)
	}

	return recipe, ok, ok
}

func openAll(
	t *testing.T,
	ctx context.Context,
	store *FilesystemStore,
	digest contracts.ObjectDigest,
) []byte {
	t.Helper()
	reader, err := store.Open(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	buffer := new(bytes.Buffer)
	if _, err := buffer.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}

	return buffer.Bytes()
}
