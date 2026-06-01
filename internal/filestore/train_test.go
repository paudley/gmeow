// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"errors"
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
			Reader:       bytes.NewReader([]byte(body)),
			MediaType:    "text/rfc822-headers",
			ContentRoles: []string{contracts.MailHeadersRole},
			Facets:       []contracts.Facet{{Kind: "email_part"}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	report, err := store.TrainDictionary(ctx, TrainDictionaryRequest{
		Family: DictionaryFamilyMailHeaders,
	})
	if err != nil {
		t.Fatal(err)
	}
	if report.Family != DictionaryFamilyMailHeaders ||
		report.DictionaryID == "" ||
		report.DictionaryBytes <= 0 ||
		report.Samples < minDictSamples {
		t.Fatalf("unexpected training report: %#v", report)
	}

	// The current marker now points at the trained dictionary, and the cache
	// reloaded so a freshly stored chunk records the new dictionary id.
	id, dict, err := store.activeDictionary(DictionaryFamilyMailHeaders)
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
		MediaType:    "text/rfc822-headers",
		ContentRoles: []string{contracts.MailHeadersRole},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
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
	if entry.DictFamily != DictionaryFamilyMailHeaders {
		t.Fatalf("new chunk recorded family %q", entry.DictFamily)
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

func TestTrainDictionaryEmptyRequestTrainsAllFamilies(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd binary not available")
	}

	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	for index := range 32 {
		body := fmt.Sprintf(
			"From: sender%[1]d@example.test\r\nTo: list@example.test\r\n"+
				"Subject: all families %[1]d\r\n\r\nHello team, item %[1]d update.\r\n",
			index,
		)
		if _, err := store.Put(ctx, PutRequest{
			Reader:       bytes.NewReader([]byte(body)),
			MediaType:    "text/rfc822-headers",
			ContentRoles: []string{contracts.MailHeadersRole},
			Facets:       []contracts.Facet{{Kind: "email_part"}},
		}); err != nil {
			t.Fatal(err)
		}
	}

	report, err := store.TrainDictionary(ctx, TrainDictionaryRequest{})
	if err != nil {
		t.Fatal(err)
	}
	if len(report.Results) == 0 {
		t.Fatalf("expected family training results: %#v", report)
	}
	if report.Results[0].Family != DictionaryFamilyMailHeaders {
		t.Fatalf("unexpected family results: %#v", report.Results)
	}
}

func TestTrainDictionaryTooFewSamplesUsesSentinel(t *testing.T) {
	if _, err := exec.LookPath("zstd"); err != nil {
		t.Skip("zstd binary not available")
	}

	store := NewFilesystemStore(t.TempDir())
	t.Cleanup(func() { _ = store.Close() })

	_, err := store.TrainDictionary(context.Background(), TrainDictionaryRequest{
		Family: DictionaryFamilyMailHeaders,
	})
	if !errors.Is(err, ErrTooFewSamples) {
		t.Fatalf("TrainDictionary error = %v, want ErrTooFewSamples", err)
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
			file.DictFamily != DictionaryFamilyMailHeaders ||
			file.ChunkCount == 0 {
			t.Fatalf("unexpected dictionary storage row: %#v", file)
		}

		return
	}
	t.Fatalf("missing dictionary storage row for %s: %#v", digest, report.Files)
}
