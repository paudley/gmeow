// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"math"
	"testing"
)

// stubEmbedder returns a deterministic unit vector per text and counts how many
// texts it was asked to embed — so a test can assert the cache short-circuits
// repeat work (the new-information detector property).
type stubEmbedder struct {
	calls int
	dim   int
}

func (s *stubEmbedder) Embed(_ context.Context, texts []string) ([]Vector, error) {
	s.calls += len(texts)
	out := make([]Vector, len(texts))
	for i, text := range texts {
		out[i] = deterministicVector(text, s.dim)
	}

	return out, nil
}

// deterministicVector hashes text into a stable pseudo-vector so equal texts map
// to equal vectors and distinct texts diverge.
func deterministicVector(text string, dim int) Vector {
	v := make(Vector, dim)
	var seed uint32 = 2166136261
	for _, b := range []byte(text) {
		seed = (seed ^ uint32(b)) * 16777619
	}
	for i := range v {
		seed = seed*1664525 + 1013904223
		v[i] = float32(int32(seed%2000)-1000) / 1000.0
	}

	return Normalize(v)
}

func TestResolverCacheIsNewInfoDetector(t *testing.T) {
	embedder := &stubEmbedder{dim: FullDim}
	resolver := NewResolver(NewMemoryCache(), embedder)
	ctx := context.Background()

	claims := []string{"name:patrick audley", "email:paudley@blackcat.ca", "org:blackcat"}

	_, misses, err := resolver.Vectors(ctx, claims)
	if err != nil {
		t.Fatalf("first Vectors: %v", err)
	}
	if misses != 3 || embedder.calls != 3 {
		t.Fatalf("first pass: misses=%d embedder.calls=%d, want 3/3", misses, embedder.calls)
	}

	// Re-ingest the same claims plus one new claim: only the new claim should
	// reach the embedder — the rest are cache hits (the NOOP/delta property).
	again := []string{"name:patrick audley", "email:paudley@blackcat.ca", "org:blackcat", "email:pat@new.example"}
	_, misses, err = resolver.Vectors(ctx, again)
	if err != nil {
		t.Fatalf("second Vectors: %v", err)
	}
	if misses != 1 {
		t.Fatalf("second pass misses=%d, want 1 (only the new claim)", misses)
	}
	if embedder.calls != 4 {
		t.Fatalf("embedder.calls=%d, want 4 total (3 + 1 new)", embedder.calls)
	}

	// A fully-redundant re-ingest must hit the embedder zero times == NOOP signal.
	_, misses, err = resolver.Vectors(ctx, claims)
	if err != nil {
		t.Fatalf("third Vectors: %v", err)
	}
	if misses != 0 {
		t.Fatalf("redundant re-ingest misses=%d, want 0 (NOOP)", misses)
	}
}

func TestMeanPoolIsUnitLength(t *testing.T) {
	a := deterministicVector("a", 16)
	b := deterministicVector("b", 16)
	pooled, err := MeanPool([]Vector{a, b}, []float64{2, 1})
	if err != nil {
		t.Fatalf("MeanPool: %v", err)
	}
	var norm float64
	for _, x := range pooled {
		norm += float64(x) * float64(x)
	}
	if math.Abs(math.Sqrt(norm)-1.0) > 1e-5 {
		t.Fatalf("pooled vector norm=%v, want ~1", math.Sqrt(norm))
	}
}

func TestEntityIndexLayeredMatchAndPersistence(t *testing.T) {
	idx := NewEntityIndex(FullDim, CoarseDim)

	paudley := deterministicVector("entity:paudley centroid", FullDim)
	other := deterministicVector("entity:someone-else centroid", FullDim)
	if err := idx.Upsert("01PAUDLEY", paudley); err != nil {
		t.Fatalf("upsert paudley: %v", err)
	}
	if err := idx.Upsert("01OTHER", other); err != nil {
		t.Fatalf("upsert other: %v", err)
	}
	if err := idx.AddName("01PAUDLEY", "01PAUDLEY:n1", deterministicVector("name:patrick audley", FullDim)); err != nil {
		t.Fatalf("add name: %v", err)
	}

	// A query near paudley's centroid should rank paudley first.
	matches, err := idx.Search(paudley, 2)
	if err != nil {
		t.Fatalf("search: %v", err)
	}
	if len(matches) == 0 || matches[0].Entity != "01PAUDLEY" {
		t.Fatalf("top match=%+v, want 01PAUDLEY first", matches)
	}
	if matches[0].Similarity < 0.99 {
		t.Fatalf("self-similarity=%v, want ~1", matches[0].Similarity)
	}

	// Round-trip through the FILESTORE artifact form must preserve matches + names.
	snap, err := idx.Snapshot()
	if err != nil {
		t.Fatalf("snapshot: %v", err)
	}
	loaded, err := LoadEntityIndex(snap)
	if err != nil {
		t.Fatalf("load: %v", err)
	}
	if loaded.Len() != idx.Len() {
		t.Fatalf("loaded.Len=%d, want %d", loaded.Len(), idx.Len())
	}
	matches, err = loaded.Search(paudley, 1)
	if err != nil || len(matches) == 0 || matches[0].Entity != "01PAUDLEY" {
		t.Fatalf("post-load search=%+v err=%v, want 01PAUDLEY", matches, err)
	}
	sim, ok := loaded.NearestName("01PAUDLEY", deterministicVector("name:patrick audley", FullDim), 8)
	if !ok || sim < 0.99 {
		t.Fatalf("post-load NearestName sim=%v ok=%v, want ~1/true", sim, ok)
	}
}
