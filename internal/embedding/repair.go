// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"sort"
)

// RepairParams configures the global REPAIR re-partition.
type RepairParams struct {
	// Threshold / NameThreshold are the idDiff value-match thresholds (reuse the
	// ingest calibration). MergeThreshold is the minimum POSITIVE signed edge weight
	// (ω-mass) for two entities to be considered the same; TopN is the blocking
	// fan-out per entity. A non-positive field falls back to a Service default.
	Threshold, NameThreshold float64
	MergeThreshold           float64
	TopN                     int
}

// RepairResult summarizes one REPAIR pass.
type RepairResult struct {
	EntitiesBefore int
	EntitiesAfter  int
	Merges         int // member entities folded into a canonical
	Clusters       int // multi-member clusters formed
}

// Repair runs the global correlation-clustering REPAIR pass
// (docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4: identity is the partition
// that maximizes global signed agreement, not the greedy pairwise ingest decision).
//
// It (1) builds a signed edge graph over all entities via ScoreEdges in entity↔entity
// mode (disjoint-set negative evidence on; veto = hard cannot-link), (2) clusters by
// constrained single-linkage — strongest positive edges first, never uniting two
// clusters that hold a cannot-link pair (which would re-create the transitivity
// violation §4 warns about) — and (3) reassigns each cluster's members onto a
// canonical entity (the min ULID), folding claims + name vectors and re-pooling
// centroids. It is MERGE-focused (consolidating the conservative ingest's over-split,
// the safe direction); entity splits are a later refinement. Deterministic.
// repairMaxRounds caps the EM-like fixpoint iteration (a backstop; convergence is
// typically a handful of rounds since each round only shrinks the entity set).
const repairMaxRounds = 12

func (s *Service) Repair(ctx context.Context, p RepairParams) (RepairResult, error) {
	if p.TopN <= 0 {
		p.TopN = s.blockingTopN
	}
	if p.MergeThreshold <= 0 {
		p.MergeThreshold = s.mergeGate
	}

	before := len(s.entityIDs())
	totalMerges, totalClusters := 0, 0

	// Iterate to a fixpoint: a merge re-folds claim sets and shifts centroids, which
	// can expose further merges (the §4.1 "assign → fold → re-score → re-assign, to
	// convergence"). Stop when a round reassigns nothing.
	for round := 0; round < repairMaxRounds; round++ {
		merges, clusters, err := s.repairRound(ctx, p)
		if err != nil {
			return RepairResult{}, err
		}
		if merges == 0 {
			break
		}
		totalMerges += merges
		totalClusters += clusters
	}

	return RepairResult{
		EntitiesBefore: before,
		EntitiesAfter:  len(s.entityIDs()),
		Merges:         totalMerges,
		Clusters:       totalClusters,
	}, nil
}

// repairRound runs one assign→fold pass: score edges, cluster, reassign. Returns
// the number of member reassignments and multi-member clusters this round.
func (s *Service) repairRound(
	ctx context.Context,
	p RepairParams,
) (merges, clusters int, err error) {
	entities := s.entityIDs()

	positive, cannotLink, err := s.repairEdges(ctx, entities, p)
	if err != nil {
		return 0, 0, err
	}

	grouped := clusterByConstrainedLinkage(entities, positive, cannotLink)

	assignments := map[string]string{} // member -> canonical (changed only)
	multi := 0
	for _, members := range grouped {
		if len(members) <= 1 {
			continue
		}
		multi++
		canonical := members[0] // sorted; min ULID is canonical
		for _, m := range members[1:] {
			assignments[m] = canonical
		}
	}

	if err := s.applyAssignments(ctx, assignments); err != nil {
		return 0, 0, err
	}

	return len(assignments), multi, nil
}

// edgeKey canonicalizes an undirected entity pair (a<b) for dedup.
type edgeKey struct{ a, b string }

func orderedKey(a, b string) edgeKey {
	if a > b {
		return edgeKey{b, a}
	}

	return edgeKey{a, b}
}

// repairEdges scores every entity against its blocked neighbours and returns the
// deduped positive edges (weight ≥ MergeThreshold) and the cannot-link set (veto).
func (s *Service) repairEdges(
	ctx context.Context,
	entities []string,
	p RepairParams,
) ([]SignedEdge, map[edgeKey]bool, error) {
	positive := []SignedEdge{}
	cannot := map[edgeKey]bool{}
	seen := map[edgeKey]bool{}

	for _, entity := range entities {
		edges, err := s.ScoreEdges(ctx, entity, p.TopN, p.Threshold, p.NameThreshold)
		if err != nil {
			return nil, nil, err
		}

		for _, edge := range edges {
			key := orderedKey(edge.A, edge.B)
			if edge.Veto {
				cannot[key] = true // a hard contradiction overrides any positive
			}
			if seen[key] {
				continue
			}
			seen[key] = true
			if edge.Weight >= p.MergeThreshold {
				positive = append(positive, SignedEdge{A: key.a, B: key.b, Weight: edge.Weight})
			}
		}
	}

	return positive, cannot, nil
}

// clusterByConstrainedLinkage merges entities along the strongest positive edges
// first, never uniting two clusters that contain a cannot-link pair. Returns the
// clusters as sorted member lists (each list sorted by ULID; the first is canonical).
func clusterByConstrainedLinkage(
	entities []string,
	positive []SignedEdge,
	cannot map[edgeKey]bool,
) [][]string {
	uf := newUnionFind(entities)
	members := make(map[string][]string, len(entities))
	for _, e := range entities {
		members[e] = []string{e}
	}

	sort.Slice(positive, func(i, j int) bool {
		if positive[i].Weight != positive[j].Weight {
			return positive[i].Weight > positive[j].Weight // strongest first
		}
		if positive[i].A != positive[j].A {
			return positive[i].A < positive[j].A
		}

		return positive[i].B < positive[j].B
	})

	cannotMerge := func(ra, rb string) bool {
		for _, x := range members[ra] {
			for _, y := range members[rb] {
				if cannot[orderedKey(x, y)] {
					return true
				}
			}
		}

		return false
	}

	for _, edge := range positive {
		ra, rb := uf.find(edge.A), uf.find(edge.B)
		if ra == rb || cannotMerge(ra, rb) {
			continue
		}

		root := uf.union(ra, rb)
		other := ra
		if root == ra {
			other = rb
		}
		members[root] = append(members[root], members[other]...)
		delete(members, other)
	}

	out := make([][]string, 0, len(members))
	for _, m := range members {
		sort.Strings(m)
		out = append(out, m)
	}
	// Deterministic cluster order (by canonical member).
	sort.Slice(out, func(i, j int) bool { return out[i][0] < out[j][0] })

	return out
}

// applyAssignments folds each member entity onto its canonical in the ledger and
// index under the resolve lock: claims and name vectors move, members are dropped,
// canonicals' centroids are re-pooled, the observation memo is re-pointed, and the
// derived ledger indexes are rebuilt. Records are NOT rewritten — the durable
// reassignment is the assignment map the caller persists for the projection fold.
func (s *Service) applyAssignments(
	ctx context.Context,
	assign map[string]string,
) error {
	if len(assign) == 0 {
		return nil
	}

	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	mergesByCanonical := map[string][]string{}
	for member, canonical := range assign {
		mergesByCanonical[canonical] = append(mergesByCanonical[canonical], member)

		canonClaims := s.ledger.claims[canonical]
		if canonClaims == nil {
			canonClaims = map[string]claimEntry{}
			s.ledger.claims[canonical] = canonClaims
		}
		for hash, entry := range s.ledger.claims[member] {
			if existing, ok := canonClaims[hash]; ok {
				entry.ValidFrom = earlierBound(existing.ValidFrom, entry.ValidFrom)
				entry.ValidUntil = laterBound(existing.ValidUntil, entry.ValidUntil)
			}
			canonClaims[hash] = entry
		}
		delete(s.ledger.claims, member)
		delete(s.entityClaims, member)
	}

	s.ledger.rebuild() // df + identifier index recomputed from the merged claim sets

	// Re-point any observation memo entry that resolved to a now-merged member.
	for obs, entity := range s.seenObs {
		if canonical, ok := assign[entity]; ok {
			s.seenObs[obs] = canonical
		}
	}

	newCentroids := make(map[string]Vector, len(mergesByCanonical))
	for canonical := range mergesByCanonical {
		delete(s.entityClaims, canonical) // claim set changed
		centroid, _, err := s.resolver.Pool(ctx, s.entityValues(canonical), nil)
		if err != nil {
			return err
		}
		newCentroids[canonical] = centroid
	}

	s.index.ApplyMerges(mergesByCanonical, newCentroids)

	return nil
}

// entityIDs returns the resolved entity ULIDs in sorted order (deterministic).
func (s *Service) entityIDs() []string {
	out := make([]string, 0, len(s.ledger.claims))
	for entity := range s.ledger.claims {
		out = append(out, entity)
	}
	sort.Strings(out)

	return out
}

// unionFind is a disjoint-set with min-ULID roots (so the canonical entity of a
// cluster is its earliest ULID — stable and deterministic).
type unionFind struct{ parent map[string]string }

func newUnionFind(entities []string) *unionFind {
	parent := make(map[string]string, len(entities))
	for _, e := range entities {
		parent[e] = e
	}

	return &unionFind{parent: parent}
}

func (u *unionFind) find(x string) string {
	for u.parent[x] != x {
		u.parent[x] = u.parent[u.parent[x]] // path halving
		x = u.parent[x]
	}

	return x
}

// union merges the two sets and returns the surviving root (the smaller ULID).
func (u *unionFind) union(a, b string) string {
	ra, rb := u.find(a), u.find(b)
	if ra == rb {
		return ra
	}
	if rb < ra {
		ra, rb = rb, ra
	}
	u.parent[rb] = ra // ra is the smaller ULID → canonical

	return ra
}
