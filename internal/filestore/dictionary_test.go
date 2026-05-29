// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestChunkDictionaryRoundTrip(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	// Configure an active raw dictionary (id 7). Raw dictionaries accept
	// arbitrary representative content as compression history.
	dictDir := filepath.Join(store.root, dictionariesDir)
	if err := os.MkdirAll(dictDir, 0o750); err != nil {
		t.Fatal(err)
	}
	dictionary := bytes.Repeat(
		[]byte("Subject: Re: common mailing-list boilerplate\n"),
		64,
	)
	if err := os.WriteFile(
		filepath.Join(dictDir, "7.dict"),
		dictionary,
		0o640,
	); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(
		filepath.Join(dictDir, currentDictMarker),
		[]byte("7"),
		0o640,
	); err != nil {
		t.Fatal(err)
	}

	digest := contracts.ObjectDigest(strings.Repeat("ab", 32))
	content := bytes.Repeat([]byte("dictionary-compressed message content sample "), 5000)
	if err := store.storeBlobContent(ctx, digest, content); err != nil {
		t.Fatal(err)
	}

	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		t.Fatalf("recipe: ok=%t err=%v", ok, err)
	}
	entry, found, err := store.lookupChunk(recipe.ChunkHashes[0])
	if err != nil || !found {
		t.Fatalf("chunk index: found=%t err=%v", found, err)
	}
	if entry.DictID != "7" {
		t.Fatalf("expected chunk to record dict id 7, got %q", entry.DictID)
	}

	got, ok, err := store.readBlobContent(digest)
	if err != nil || !ok {
		t.Fatalf("read: ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("dictionary-compressed content did not round-trip")
	}
}

func TestChunkWithoutDictionaryRecordsEmptyID(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	digest := contracts.ObjectDigest(strings.Repeat("cd", 32))
	content := bytes.Repeat([]byte("no dictionary configured here "), 4000)
	if err := store.storeBlobContent(ctx, digest, content); err != nil {
		t.Fatal(err)
	}

	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		t.Fatalf("recipe: ok=%t err=%v", ok, err)
	}
	entry, found, err := store.lookupChunk(recipe.ChunkHashes[0])
	if err != nil || !found {
		t.Fatalf("chunk index: found=%t err=%v", found, err)
	}
	if entry.DictID != "" {
		t.Fatalf(
			"expected empty dict id without a configured dictionary, got %q",
			entry.DictID,
		)
	}

	got, ok, err := store.readBlobContent(digest)
	if err != nil || !ok || !bytes.Equal(got, content) {
		t.Fatalf("plain content did not round-trip: ok=%t err=%v", ok, err)
	}
}
