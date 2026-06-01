// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"fmt"
	"os/exec"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestTrainDictionaryInstallsAndAdopts(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd binary not available")
	}

	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	// Store many small, similar objects — the workload a dictionary helps most.
	for index := range 64 {
		body := fmt.Sprintf(
			"From: sender%[1]d@example.test\r\nTo: list@example.test\r\n"+
				"Subject: weekly status %[1]d\r\n\r\nHello team, item %[1]d update.\r\n",
			index,
		)
		if _, err := store.Put(ctx, PutRequest{
			Reader: bytes.NewReader([]byte(body)),
			Facets: []contracts.Facet{{Kind: "email_part"}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	report, err := store.TrainDictionary(ctx, 0)
	if err != nil {
		t.Fatal(err)
	}
	if report.DictionaryID == "" || report.DictionaryBytes <= 0 ||
		report.Samples < minDictSamples {
		t.Fatalf("unexpected training report: %#v", report)
	}

	// The current marker now points at the trained dictionary, and the cache
	// reloaded so a freshly stored chunk records the new dictionary id.
	id, dict, err := store.activeDictionary()
	if err != nil {
		t.Fatal(err)
	}
	if id != report.DictionaryID || len(dict) == 0 {
		t.Fatalf("active dictionary not updated: id=%q bytes=%d", id, len(dict))
	}

	digest, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(
			[]byte("From: new@example.test\r\nSubject: adopt\r\n\r\nnew body\r\n"),
		),
		Facets: []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
		t.Fatal(err)
	}
	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		t.Fatalf("expected recipe: ok=%t err=%v", ok, err)
	}
	entry, ok, err := store.lookupChunk(recipe.ChunkHashes[0])
	if err != nil || !ok {
		t.Fatalf("expected chunk index entry: ok=%t err=%v", ok, err)
	}
	if entry.DictID != report.DictionaryID {
		t.Fatalf(
			"new chunk did not adopt dictionary: dict_id=%q want=%q",
			entry.DictID,
			report.DictionaryID,
		)
	}
	breakdown, err := store.StorageBreakdown(ctx, StorageBreakdownRequest{
		Digest: digest,
	})
	if err != nil {
		t.Fatal(err)
	}
	assertDictionaryStorageRow(t, breakdown, digest, report.DictionaryID)

	// And the object still reads back intact through the dictionary decoder.
	got := openAll(t, ctx, store, digest)
	if !bytes.Contains(got, []byte("new body")) {
		t.Fatalf("dictionary-compressed object did not round-trip: %q", got)
	}
}

func assertDictionaryStorageRow(
	t *testing.T,
	report StorageBreakdownReport,
	digest contracts.ObjectDigest,
	dictID string,
) {
	t.Helper()
	for _, file := range report.Files {
		if file.ObjectDigest != digest || file.Role != "content" {
			continue
		}
		if file.DictID != dictID ||
			len(file.DictIDs) != 1 ||
			file.DictIDs[0] != dictID ||
			file.ChunkCount == 0 {
			t.Fatalf("unexpected dictionary storage row: %#v", file)
		}

		return
	}
	t.Fatalf("missing dictionary storage row for %s: %#v", digest, report.Files)
}
