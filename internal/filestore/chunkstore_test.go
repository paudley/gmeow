// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"bytes"
	"context"
	"io"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

// TestStreamingChunkerMatchesInMemory locks the dedup-critical invariant that
// the streaming chunker produces exactly the same content-defined boundaries as
// the in-memory chunkContent, so objects stored via either path share chunks.
func TestStreamingChunkerMatchesInMemory(t *testing.T) {
	content := bytes.Repeat([]byte("streaming vs in-memory chunk parity 9876 "), 4000)

	var inMemory []string
	for _, chunk := range chunkContent(content) {
		inMemory = append(inMemory, blake3HexBytes(chunk))
	}

	var streamed []string
	total, err := streamChunks(bytes.NewReader(content), func(chunk []byte) error {
		streamed = append(streamed, blake3HexBytes(chunk))

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if total != int64(len(content)) {
		t.Fatalf("streamChunks total = %d, want %d", total, len(content))
	}
	if len(inMemory) != len(streamed) {
		t.Fatalf(
			"chunk count differs: in-memory=%d streamed=%d",
			len(inMemory),
			len(streamed),
		)
	}
	for index := range inMemory {
		if inMemory[index] != streamed[index] {
			t.Fatalf("chunk %d differs: in-memory=%s streamed=%s",
				index, inMemory[index], streamed[index])
		}
	}
}

// TestStreamingStoreMatchesInMemoryDigest confirms the streaming store path
// yields the same object digest and recipe length as the in-memory path.
func TestStreamingStoreMatchesInMemoryDigest(t *testing.T) {
	ctx := context.Background()
	content := bytes.Repeat([]byte("identity parity across store paths "), 3000)

	memStore := NewFilesystemStore(t.TempDir())
	memDigest := contracts.ObjectDigest(blake3HexBytes(content))
	if err := memStore.storeBlobContent(ctx, memDigest, content, "", nil); err != nil {
		t.Fatal(err)
	}
	memRecipe, _, err := memStore.readRecipe(memDigest)
	if err != nil {
		t.Fatal(err)
	}

	streamStore := NewFilesystemStore(t.TempDir())
	streamDigest, _, size, err := streamStore.storeBlobReader(
		ctx,
		bytes.NewReader(content),
		"",
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if streamDigest != memDigest {
		t.Fatalf("streamed digest %s != in-memory digest %s", streamDigest, memDigest)
	}
	if size != int64(len(content)) {
		t.Fatalf("streamed size %d != %d", size, len(content))
	}
	streamRecipe, _, err := streamStore.readRecipe(streamDigest)
	if err != nil {
		t.Fatal(err)
	}
	if len(streamRecipe.ChunkHashes) != len(memRecipe.ChunkHashes) {
		t.Fatalf("recipe length differs: streamed=%d in-memory=%d",
			len(streamRecipe.ChunkHashes), len(memRecipe.ChunkHashes))
	}
}

// TestOpenStreamingSurfacesMissingChunk confirms the streaming Open reader
// propagates a chunk-read failure instead of returning truncated content.
func TestOpenStreamingSurfacesMissingChunk(t *testing.T) {
	ctx := context.Background()
	store := NewFilesystemStore(t.TempDir())
	content := bytes.Repeat([]byte("corruption detection on streaming read "), 3000)
	digest, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(content),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		t.Fatalf("expected recipe: ok=%t err=%v", ok, err)
	}
	// Drop a chunk's index entry so reassembly cannot find it.
	if err := store.metaDelete(chunkIndexKey(recipe.ChunkHashes[0])); err != nil {
		t.Fatal(err)
	}

	reader, err := store.Open(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	if _, err := io.Copy(io.Discard, reader); err == nil {
		t.Fatal("expected streaming read to fail on a missing chunk")
	}
}

func TestChunkStoreRoundTrip(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest := contracts.ObjectDigest(strings.Repeat("ab", 32))

	content := bytes.Repeat([]byte("the quick brown fox jumps 0123456789 "), 5000)

	if err := store.storeBlobContent(ctx, digest, content, "", nil); err != nil {
		t.Fatal(err)
	}

	if ok, err := store.hasRecipe(digest); err != nil || !ok {
		t.Fatalf("expected recipe to exist: ok=%t err=%v", ok, err)
	}

	got, ok, err := store.readBlobContent(digest)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected chunked content to be readable")
	}
	if !bytes.Equal(got, content) {
		t.Fatalf("round-trip mismatch: got %d bytes, want %d", len(got), len(content))
	}
}

func TestChunkStoreDedupsRepeatedContent(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()
	digest := contracts.ObjectDigest(strings.Repeat("cd", 32))

	// A repeated 32KB block produces a repeating chunk sequence that must
	// dedup: the recipe references far fewer distinct chunks than total.
	block := bytes.Repeat([]byte("abcdefgh"), 4096)
	content := bytes.Repeat(block, 8)

	if err := store.storeBlobContent(ctx, digest, content, "", nil); err != nil {
		t.Fatal(err)
	}

	recipe, ok, err := store.readRecipe(digest)
	if err != nil || !ok {
		t.Fatalf("expected recipe: ok=%t err=%v", ok, err)
	}

	distinct := map[string]struct{}{}
	for _, hash := range recipe.ChunkHashes {
		distinct[hash] = struct{}{}
	}
	if len(recipe.ChunkHashes) < 2 {
		t.Fatalf("expected multiple chunks, got %d", len(recipe.ChunkHashes))
	}
	if len(distinct) >= len(recipe.ChunkHashes) {
		t.Fatalf(
			"expected dedup: %d distinct of %d chunks",
			len(distinct),
			len(recipe.ChunkHashes),
		)
	}

	got, ok, err := store.readBlobContent(digest)
	if err != nil || !ok {
		t.Fatalf("read failed: ok=%t err=%v", ok, err)
	}
	if !bytes.Equal(got, content) {
		t.Fatal("round-trip mismatch after dedup")
	}
}

func TestPutAndOpenUsesChunkStore(t *testing.T) {
	store := NewFilesystemStore(t.TempDir())
	ctx := context.Background()

	content := bytes.Repeat([]byte("phase two content addressed store "), 2000)
	digest, err := store.Put(ctx, PutRequest{
		Reader: bytes.NewReader(content),
		Facets: []contracts.Facet{{Kind: "file"}},
	})
	if err != nil {
		t.Fatal(err)
	}

	// Content lives in the chunk store: the put object must have a recipe.
	if _, ok, recipeErr := store.readRecipe(digest); recipeErr != nil || !ok {
		t.Fatalf("expected a recipe for the put object: ok=%t err=%v", ok, recipeErr)
	}

	reader, err := store.Open(ctx, digest)
	if err != nil {
		t.Fatal(err)
	}
	defer reader.Close()
	got := new(bytes.Buffer)
	if _, err := got.ReadFrom(reader); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(got.Bytes(), content) {
		t.Fatal("Open did not reassemble chunked content")
	}
}
