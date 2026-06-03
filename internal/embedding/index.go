// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"sort"
	"sync"

	"github.com/coder/hnsw"
)

// EntityIndex is the in-process ANN over entity vectors used for ingest-time
// resolution. It holds three coder/hnsw graphs keyed by entity ULID:
//   - coarse: 128-d Matryoshka profile centroids, for cheap recall/blocking
//   - fine:   768-d profile centroids, for precision (a free Matryoshka slice)
//   - names:  768-d name vectors (one entity may own several — renames add more),
//     keyed name-key -> entity, for identity confirmation
//
// The whole index is persisted as a single FILESTORE artifact (Export/Snapshot),
// so the importer loads it in-process and never touches QUERY. Centroids are
// materialized views — rebuildable from the immutable record log — so a corrupt
// or stale index is always recoverable by replay.
// rebuildEvery is how many centroid UPDATES (re-pools of existing entities)
// accumulate before the fine/coarse graphs are rebuilt from the authoritative
// centroid map. New entities are added to the graphs immediately, so they are
// always findable; only updated centroids go briefly stale between rebuilds.
const rebuildEvery = 256

type EntityIndex struct {
	coarse      *hnsw.Graph[string]
	fine        *hnsw.Graph[string]
	centroids   map[string]Vector
	entityNames map[string][]Vector
	dim         int
	dimC        int
	dirty       int
	mu          sync.RWMutex
}

// NewEntityIndex builds an empty index for the given full/coarse dimensions
// (default FullDim/CoarseDim when zero).
func NewEntityIndex(fullDim, coarseDim int) *EntityIndex {
	if fullDim <= 0 {
		fullDim = FullDim
	}

	if coarseDim <= 0 || coarseDim > fullDim {
		coarseDim = CoarseDim
	}

	return &EntityIndex{
		dim:         fullDim,
		dimC:        coarseDim,
		coarse:      hnsw.NewGraph[string](),
		fine:        hnsw.NewGraph[string](),
		centroids:   make(map[string]Vector),
		entityNames: make(map[string][]Vector),
	}
}

// Match is one candidate entity ranked by the layered search.
type Match struct {
	Entity     string
	Similarity float64 // cosine similarity in [-1,1]; higher is closer
}

// Upsert sets the profile centroid for an entity ULID. centroid must be full
// dimension. A new entity is added to the ANN graphs immediately; an update to
// an existing entity rewrites the authoritative map and defers the graph refresh
// to a periodic rebuild (avoiding the Delete+Add churn that corrupts coder/hnsw).
// A non-finite or zero centroid is rejected (it would poison cosine search).
func (idx *EntityIndex) Upsert(entity string, centroid Vector) error {
	if len(centroid) != idx.dim {
		return fmt.Errorf("centroid dim %d != index dim %d", len(centroid), idx.dim)
	}

	fine := Normalize(centroid)
	if !isUsableVector(fine) {
		return fmt.Errorf("refusing to index a zero/non-finite centroid for %s", entity)
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	_, existed := idx.centroids[entity]

	idx.centroids[entity] = fine
	if !existed {
		idx.fine.Add(hnsw.MakeNode(entity, fine))
		idx.coarse.Add(hnsw.MakeNode(entity, Slice(fine, idx.dimC)))

		return nil
	}

	idx.dirty++
	if idx.dirty >= rebuildEvery {
		idx.rebuildLocked()
	}

	return nil
}

// rebuildLocked rebuilds the fine/coarse graphs from the authoritative centroid
// map, picking up drifted centroids. Caller must hold idx.mu.
func (idx *EntityIndex) rebuildLocked() {
	fine := hnsw.NewGraph[string]()
	coarse := hnsw.NewGraph[string]()

	for entity, centroid := range idx.centroids {
		fine.Add(hnsw.MakeNode(entity, centroid))
		coarse.Add(hnsw.MakeNode(entity, Slice(centroid, idx.dimC)))
	}

	idx.fine = fine
	idx.coarse = coarse
	idx.dirty = 0
}

func isUsableVector(v Vector) bool {
	var sum float64

	for _, x := range v {
		f := float64(x)
		if f != f || f > 1e308 || f < -1e308 { // NaN or ±Inf
			return false
		}

		sum += f * f
	}

	return sum > 0
}

// AddName attaches a name vector to an entity (renames accumulate several).
// nameKey is unused now that names are stored per entity, but kept for caller
// compatibility. vector must be full dimension; zero/non-finite vectors skip.
func (idx *EntityIndex) AddName(entity, _ string, vector Vector) error {
	if len(vector) != idx.dim {
		return fmt.Errorf("name vector dim %d != index dim %d", len(vector), idx.dim)
	}

	normalized := Normalize(vector)
	if !isUsableVector(normalized) {
		return nil
	}

	idx.mu.Lock()
	defer idx.mu.Unlock()

	idx.entityNames[entity] = append(idx.entityNames[entity], normalized)

	return nil
}

// Search runs the layered match: coarse 128-d recall → fine 768-d precision over
// the recalled set. It returns up to k fine-ranked candidates. The caller (the
// resolver) applies thresholds and may fold in a name-vector or comm-graph check.
func (idx *EntityIndex) Search(centroid Vector, k int) ([]Match, error) {
	if len(centroid) != idx.dim {
		return nil, fmt.Errorf("query dim %d != index dim %d", len(centroid), idx.dim)
	}

	if k <= 0 {
		k = 1
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	if idx.fine.Len() == 0 {
		return nil, nil
	}

	// Coarse recall over a wider beam, then re-rank with the fine centroid.
	beam := max(k*4, 16)
	coarseHits := idx.coarse.Search(Slice(centroid, idx.dimC), beam)
	fineQuery := Normalize(centroid)

	matches := make([]Match, 0, len(coarseHits))
	for _, hit := range coarseHits {
		vec, ok := idx.fine.Lookup(hit.Key)
		if !ok {
			continue
		}

		matches = append(
			matches,
			Match{Entity: hit.Key, Similarity: cosineSimilarity(fineQuery, vec)},
		)
	}

	sortMatchesDesc(matches)

	if len(matches) > k {
		matches = matches[:k]
	}

	return matches, nil
}

// NearestName returns the best cosine similarity between the query and the
// entity's OWN name vectors, and whether the entity has any. This is a direct
// comparison (no global ANN), so it correctly disconfirms a centroid match whose
// name diverges from the entity's. beam is ignored (kept for signature stability).
func (idx *EntityIndex) NearestName(
	entity string,
	query Vector,
	_ int,
) (float64, bool) {
	if len(query) != idx.dim {
		return 0, false
	}

	idx.mu.RLock()
	defer idx.mu.RUnlock()

	names := idx.entityNames[entity]
	if len(names) == 0 {
		return 0, false
	}

	q := Normalize(query)

	best := -2.0
	for _, name := range names {
		if sim := cosineSimilarity(q, name); sim > best {
			best = sim
		}
	}

	return best, true
}

// Len reports the number of entities in the index (the authoritative centroid
// count, not the possibly-stale graph node count).
func (idx *EntityIndex) Len() int {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	return len(idx.centroids)
}

func cosineSimilarity(a, b Vector) float64 {
	// Inputs are unit vectors here, so cosine == dot product.
	var dot float64

	n := min(len(b), len(a))
	for i := range n {
		dot += float64(a[i]) * float64(b[i])
	}

	return dot
}

func sortMatchesDesc(matches []Match) {
	for i := 1; i < len(matches); i++ {
		for j := i; j > 0 && matches[j].Similarity > matches[j-1].Similarity; j-- {
			matches[j], matches[j-1] = matches[j-1], matches[j]
		}
	}
}

// Snapshot serializes the AUTHORITATIVE state — the centroid map, the name-owner
// map, and the names graph — into a single byte artifact. The fine/coarse graphs
// are NOT persisted: they are a rebuildable view, reconstructed from the centroid
// map on load. Format is internal and versioned by a magic header.
func (idx *EntityIndex) Snapshot() ([]byte, error) {
	idx.mu.RLock()
	defer idx.mu.RUnlock()

	var buf bytes.Buffer

	err := writeArtifactHeader(
		&buf,
		idx.dim,
		idx.dimC,
		len(idx.entityNames),
	)
	if err != nil {
		return nil, err
	}

	for _, entity := range sortedVectorListKeys(idx.entityNames) {
		err := writeLenString(&buf, entity)
		if err != nil {
			return nil, err
		}

		vectors := idx.entityNames[entity]

		err = binary.Write(&buf, binary.LittleEndian, uint32(len(vectors)))
		if err != nil {
			return nil, err
		}

		for _, vector := range vectors {
			err := writeVector(&buf, vector)
			if err != nil {
				return nil, err
			}
		}
	}

	err = binary.Write(
		&buf,
		binary.LittleEndian,
		uint32(len(idx.centroids)),
	)
	if err != nil {
		return nil, err
	}

	for _, entity := range sortedKeys(idx.centroids) {
		err := writeLenString(&buf, entity)
		if err != nil {
			return nil, err
		}

		err = writeVector(&buf, idx.centroids[entity])
		if err != nil {
			return nil, err
		}
	}

	return buf.Bytes(), nil
}

// LoadEntityIndex reconstructs an index from a Snapshot artifact, rebuilding the
// fine/coarse ANN graphs from the persisted centroid map.
func LoadEntityIndex(data []byte) (*EntityIndex, error) {
	buf := bytes.NewReader(data)

	dim, dimC, nameCount, err := readArtifactHeader(buf)
	if err != nil {
		return nil, err
	}

	idx := NewEntityIndex(dim, dimC)

	for range nameCount {
		entity, err := readLenString(buf)
		if err != nil {
			return nil, err
		}

		var vectorCount uint32
		if err := binary.Read(buf, binary.LittleEndian, &vectorCount); err != nil {
			return nil, err
		}

		vectors := make([]Vector, 0, vectorCount)
		for range vectorCount {
			vector, err := readVector(buf)
			if err != nil {
				return nil, err
			}

			vectors = append(vectors, vector)
		}

		idx.entityNames[entity] = vectors
	}

	var centroidCount uint32
	if err := binary.Read(buf, binary.LittleEndian, &centroidCount); err != nil {
		return nil, err
	}

	for range centroidCount {
		entity, err := readLenString(buf)
		if err != nil {
			return nil, err
		}

		vector, err := readVector(buf)
		if err != nil {
			return nil, err
		}

		idx.centroids[entity] = vector
	}

	idx.rebuildLocked()

	return idx, nil
}

func sortedKeys(m map[string]Vector) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}

func sortedVectorListKeys(m map[string][]Vector) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	return keys
}
