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
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestDictionaryFamilyClassifier(t *testing.T) {
	for _, tc := range []struct {
		name      string
		mediaType string
		roles     []string
		want      string
	}{
		{
			name:      "mail headers role",
			mediaType: "application/octet-stream",
			roles:     []string{contracts.MailHeadersRole},
			want:      DictionaryFamilyMailHeaders,
		},
		{
			name:      "mail html body role",
			mediaType: "text/html; charset=utf-8",
			roles:     []string{contracts.MailBodyRole},
			want:      DictionaryFamilyMailBodyHTML,
		},
		{
			name:      "generic json",
			mediaType: "application/json",
			want:      DictionaryFamilyStructuredJSON,
		},
		{
			name:      "unknown json suffix",
			mediaType: "application/example+json",
			want:      DictionaryFamilyStructuredJSON,
		},
		{
			name:      "zip is opaque",
			mediaType: "application/zip",
			want:      DictionaryFamilyOpaqueBinary,
		},
		{
			name:      "image is opaque",
			mediaType: "image/png",
			want:      DictionaryFamilyOpaqueBinary,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			got := dictionaryFamilyForObject(tc.mediaType, tc.roles)
			if got != tc.want {
				t.Fatalf("family = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestChunkDictionaryRoundTrip(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	dictPath := store.dictionaryPath("7")
	if err := os.MkdirAll(filepath.Dir(dictPath), 0o750); err != nil {
		t.Fatal(err)
	}
	dictionary := bytes.Repeat(
		[]byte("Subject: Re: common mailing-list boilerplate\n"),
		64,
	)
	if err := os.WriteFile(dictPath, dictionary, 0o640); err != nil {
		t.Fatal(err)
	}
	if err := store.writeDictionaryRegistry(dictionaryRegistry{
		SchemaVersion: 1,
		Dictionaries: map[string]dictionaryRegistryRecord{
			"7": {
				ID:       "7",
				Family:   DictionaryFamilyMailHeaders,
				Version:  "test",
				Filename: filepath.Join(dictFilesDir, "7.dict"),
			},
		},
	}); err != nil {
		t.Fatal(err)
	}
	if err := atomicWriteFile(
		store.dictionaryCurrentPath(DictionaryFamilyMailHeaders),
		[]byte("7"),
		0o640,
	); err != nil {
		t.Fatal(err)
	}

	digest := contracts.ObjectDigest(strings.Repeat("ab", 32))
	content := bytes.Repeat([]byte("dictionary-compressed message content sample "), 5000)
	if err := store.storeBlobContent(
		ctx,
		digest,
		content,
		"text/rfc822-headers",
		[]string{contracts.MailHeadersRole},
	); err != nil {
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
	if entry.DictFamily != DictionaryFamilyMailHeaders {
		t.Fatalf("expected chunk to record family, got %q", entry.DictFamily)
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
	if err := store.storeBlobContent(
		ctx,
		digest,
		content,
		"application/octet-stream",
		nil,
	); err != nil {
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
	if entry.DictID != "" || entry.DictFamily != "" {
		t.Fatalf("expected no dictionary metadata, got %#v", entry)
	}

	got, ok, err := store.readBlobContent(digest)
	if err != nil || !ok || !bytes.Equal(got, content) {
		t.Fatalf("plain content did not round-trip: ok=%t err=%v", ok, err)
	}
}

func TestDefaultDictionariesBootstrapFreshStore(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	digest, err := store.Put(ctx, PutRequest{
		Reader:       strings.NewReader("From: a@example.test\r\nSubject: hello\r\n\r\n"),
		MediaType:    "text/rfc822-headers",
		ContentRoles: []string{contracts.MailHeadersRole},
		Facets:       []contracts.Facet{{Kind: "email_part"}},
	})
	if err != nil {
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
	if entry.DictID == "" || entry.DictFamily != DictionaryFamilyMailHeaders {
		t.Fatalf("fresh store did not bootstrap mail headers dictionary: %#v", entry)
	}
}

func TestLegacyGlobalDictionaryChunkRemainsReadable(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())

	dictDir := filepath.Join(store.root, dictionariesDir)
	if err := os.MkdirAll(dictDir, 0o750); err != nil {
		t.Fatal(err)
	}
	dictionary := bytes.Repeat([]byte("legacy dictionary content "), 64)
	if err := os.WriteFile(
		filepath.Join(dictDir, "9.dict"),
		dictionary,
		0o640,
	); err != nil {
		t.Fatal(err)
	}
	encoder, err := store.dictEncoder("9", dictionary)
	if err != nil {
		t.Fatal(err)
	}
	content := bytes.Repeat([]byte("legacy dictionary content payload "), 2000)
	compressed := encoder.EncodeAll(content, nil)

	store.packMu.Lock()
	packID, offset, err := store.appendToActivePack(compressed)
	store.packMu.Unlock()
	if err != nil {
		t.Fatal(err)
	}
	hash := blake3HexBytes(content)
	if err := store.metaPut(chunkIndexKey(hash), chunkIndexEntry{
		UpdatedAt: time.Now().UTC(),
		ChunkHash: hash,
		DictID:    "9",
		PackID:    packID,
		Offset:    offset,
		Length:    int64(len(compressed)),
	}); err != nil {
		t.Fatal(err)
	}
	digest := contracts.ObjectDigest(strings.Repeat("ef", 32))
	if err := store.metaPut(objectRecipeKey(digest), objectRecipeEntry{
		UpdatedAt:    time.Now().UTC(),
		Digest:       digest,
		ChunkHashes:  []string{hash},
		ContentBytes: int64(len(content)),
	}); err != nil {
		t.Fatal(err)
	}

	got, ok, err := store.readBlobContent(digest)
	if err != nil || !ok || !bytes.Equal(got, content) {
		t.Fatalf("legacy dictionary content did not round-trip: ok=%t err=%v", ok, err)
	}
}
