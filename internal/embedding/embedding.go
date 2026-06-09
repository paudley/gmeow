// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

// Package embedding is the in-process core of the EMBEDDING service: the
// claim-vector cache (keyed by statement hash), Matryoshka slicing, incremental
// weighted mean-pooling, and the coder/hnsw ANN index wrapper persisted as a
// FILESTORE artifact.
//
// It is the heart of ingest-time entity resolution (see the locked design in
// the contact ingestion plan): the importer embeds a record's claims, mean-pools
// them into a candidate vector, and matches it against persisted entity centroids
// — all without touching QUERY. The cache doubles as the new-information
// detector: a redundant re-ingest finds every statement hash already cached, so
// the embedder is never called and the work collapses to a NOOP.
package embedding

import (
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"maps"
	"math"
	"strings"
	"sync"
)

// Vector is a dense embedding. float32 matches coder/hnsw's Vector so index
// inserts and searches need no conversion; the embedding endpoint returns
// float64, converted once on ingestion via VectorFromFloat64.
type Vector = []float32

const (
	// NamespaceContactClaim is the legacy/default cache namespace for contact
	// identity claim values. It intentionally uses bare StatementHash keys so
	// existing persisted resolution states remain warm after this change.
	NamespaceContactClaim = "contact_claim"
	// NamespaceEmailSegment isolates email body/header segment embeddings from
	// contact claim vectors. Email text has a different distribution and must not
	// feed the contact residual basis used by identity resolution.
	NamespaceEmailSegment = "email_segment"
	// FullDim is the native dimensionality of nomic-embed-text-v1.5.
	FullDim = 768
	// CoarseDim is the Matryoshka prefix used for cheap coarse blocking. Slicing
	// the full vector to this prefix and renormalizing yields a usable low-dim
	// embedding for free (Matryoshka property) — no second embedder call.
	CoarseDim = 128
)

// StatementHash is the cache key for a claim's embedding: the SHA-256 of the
// exact text embedded. Identical statements across records/snapshots collapse to
// one cache entry (and one embedder call, ever).
func StatementHash(text string) string {
	sum := sha256.Sum256([]byte(text))

	return hex.EncodeToString(sum[:])
}

// CacheKey scopes a text embedding to a purpose namespace. The default/contact
// namespace deliberately preserves the historical key format for state
// compatibility; all other namespaces are prefixed.
func CacheKey(namespace, text string) string {
	hash := StatementHash(text)
	if normalizedNamespace(namespace) == NamespaceContactClaim {
		return hash
	}

	return normalizedNamespace(namespace) + "\x00" + hash
}

func normalizedNamespace(namespace string) string {
	namespace = strings.TrimSpace(namespace)
	if namespace == "" {
		return NamespaceContactClaim
	}

	return namespace
}

func cacheKeyNamespace(key string) string {
	if before, _, found := strings.Cut(key, "\x00"); found {
		return before
	}

	return NamespaceContactClaim
}

// VectorFromFloat64 converts an endpoint vector to the float32 form used
// throughout the index and cache.
func VectorFromFloat64(in []float64) Vector {
	out := make(Vector, len(in))
	for i, v := range in {
		out[i] = float32(v)
	}

	return out
}

// Normalize returns the L2-normalized copy of v. Cosine distance assumes unit
// vectors; a zero vector is returned unchanged (its norm is undefined).
func Normalize(v Vector) Vector {
	var sum float64
	for _, x := range v {
		sum += float64(x) * float64(x)
	}

	if sum == 0 {
		return append(Vector(nil), v...)
	}

	inv := float32(1.0 / math.Sqrt(sum))

	out := make(Vector, len(v))
	for i, x := range v {
		out[i] = x * inv
	}

	return out
}

// Slice returns the first dim components of v, renormalized to unit length. This
// is the Matryoshka coarse-blocking operation: a truncated prefix of a
// Matryoshka embedding is itself a valid (lower-fidelity) embedding once
// renormalized. dim<=0 or dim>=len(v) returns a normalized full-length copy.
func Slice(v Vector, dim int) Vector {
	if dim <= 0 || dim >= len(v) {
		return Normalize(v)
	}

	return Normalize(v[:dim])
}

// MeanPool computes the weighted mean of vectors, renormalized to unit length —
// the profile centroid of a set of claim vectors. weights may be nil (uniform);
// otherwise len(weights) must equal len(vectors). Vectors of differing length or
// an empty input yield an error: a centroid is only meaningful over same-dim
// claim vectors.
func MeanPool(vectors []Vector, weights []float64) (Vector, error) {
	if len(vectors) == 0 {
		return nil, errors.New("mean-pool requires at least one vector")
	}

	if weights != nil && len(weights) != len(vectors) {
		return nil, errors.New("mean-pool weights must match vectors")
	}

	dim := len(vectors[0])
	acc := make([]float64, dim)

	var totalWeight float64

	for i, v := range vectors {
		if len(v) != dim {
			return nil, errors.New("mean-pool requires equal-dimension vectors")
		}

		w := 1.0
		if weights != nil {
			w = weights[i]
		}

		if w == 0 {
			continue
		}

		for j, x := range v {
			acc[j] += w * float64(x)
		}

		totalWeight += w
	}

	if totalWeight == 0 {
		return nil, errors.New("mean-pool total weight is zero")
	}

	out := make(Vector, dim)
	for j, x := range acc {
		out[j] = float32(x / totalWeight)
	}

	return Normalize(out), nil
}

// Cache stores full-dimension claim vectors keyed by statement hash. It is the
// new-information detector for ingest: Get-miss == genuinely-new claim.
type Cache interface {
	Get(hash string) (Vector, bool)
	Put(hash string, vector Vector)
	Len() int
}

// MemoryCache is a concurrency-safe in-memory Cache. The EMBEDDING service holds
// one; it may be snapshotted to a FILESTORE artifact for warm restarts.
type MemoryCache struct {
	entries map[string]Vector
	mu      sync.RWMutex
}

func NewMemoryCache() *MemoryCache {
	return &MemoryCache{entries: make(map[string]Vector)}
}

func (c *MemoryCache) Get(hash string) (Vector, bool) {
	c.mu.RLock()
	defer c.mu.RUnlock()

	v, ok := c.entries[hash]

	return v, ok
}

func (c *MemoryCache) Put(hash string, vector Vector) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries[hash] = vector
}

func (c *MemoryCache) Len() int {
	c.mu.RLock()
	defer c.mu.RUnlock()

	return len(c.entries)
}

// snapshot returns a shallow copy of the cache entries for serialization.
func (c *MemoryCache) snapshot() map[string]Vector {
	c.mu.RLock()
	defer c.mu.RUnlock()

	out := make(map[string]Vector, len(c.entries))
	maps.Copy(out, c.entries)

	return out
}

// load replaces the cache contents with the given entries.
func (c *MemoryCache) load(entries map[string]Vector) {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.entries = make(map[string]Vector, len(entries))
	maps.Copy(c.entries, entries)
}
