// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"sync"
)

// Service is the in-process facade the EMBEDDING gRPC server delegates to. It
// composes the caching Resolver (claim vectors) and the EntityIndex (ANN over
// centroids + name vectors), so all embedding/resolution logic lives here and the
// RPC layer stays a thin marshaller.
type Service struct {
	resolver *Resolver
	index    *EntityIndex
	ledger   *entityLedger
	newID    func() string
	model    string
	// idDiff tunables (docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4.1).
	blockingTopN int     // HNSW candidate fan-out before idDiff re-rank
	mergeGate    float64 // min idDiff signed score (ω-mass units) to merge vs mint
	lambda       float64 // IAC penalty weight in the signed score
	setPenalty   float64 // scale for disjoint-set negative evidence (repair mode)
	vetoMass     float64 // functional-contradiction ω that hard-vetoes a merge
	tauCtx       float64 // contextual cosine value-match threshold
	// seenObs memoizes observation-fingerprint -> resolved entity. It makes
	// resolution deterministic and idempotent for an IDENTICAL observation
	// (same claim set), independent of centroid drift or blocking recall — a
	// re-ingest returns the same entity as a NOOP and never mints a duplicate.
	seenObs map[string]string
	// resolveMu serializes Resolve so the match->mint->diff->upsert read-modify-
	// write of the index+ledger is atomic (import is sequential; concurrent gRPC
	// Resolve calls thus queue rather than racing the entity space).
	resolveMu sync.Mutex
}

// idDiff tunable defaults. mergeGate is a raw signed score in ω-mass units: ~one
// strong functional match (a full-name agreement, base ω 1.0) or a couple of
// identifier matches clears it; a lone weak contextual match does not. Tuned
// empirically against the corpus.
const (
	defaultBlockingTopN = 24
	defaultMergeGate    = 0.75
	defaultLambda       = 1.5
	defaultSetPenalty   = 0.5
	defaultVetoMass     = 4.0
	defaultTauCtx       = 0.6
)

// NamedVec is one name vector attached to an entity (key unique per vector).
type NamedVec struct {
	Key    string
	Vector Vector
}

func NewService(resolver *Resolver, index *EntityIndex, model string) *Service {
	if index == nil {
		index = NewEntityIndex(FullDim, CoarseDim)
	}

	return &Service{
		resolver:     resolver,
		index:        index,
		ledger:       newEntityLedger(),
		newID:        defaultIDSource(),
		model:        model,
		blockingTopN: defaultBlockingTopN,
		mergeGate:    defaultMergeGate,
		lambda:       defaultLambda,
		setPenalty:   defaultSetPenalty,
		vetoMass:     defaultVetoMass,
		tauCtx:       defaultTauCtx,
		seenObs:      make(map[string]string),
	}
}

// SetIDSource overrides the entity-ULID generator (tests use a deterministic
// counter). It is not concurrency-safe; call before serving.
func (s *Service) SetIDSource(fn func() string) {
	if fn != nil {
		s.newID = fn
	}
}

// ResetEntities clears the resolved entity space (index + ledger) but KEEPS the
// claim-vector cache, so a threshold sweep can re-resolve the same corpus at a
// new threshold without paying the embedding cost again.
func (s *Service) ResetEntities() {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	s.index = NewEntityIndex(FullDim, CoarseDim)
	s.ledger = newEntityLedger()
	s.seenObs = make(map[string]string)
}

// Index exposes the underlying entity index (used in-process by ingest before
// the gRPC tier is involved).
func (s *Service) Index() *EntityIndex { return s.index }

// Resolver exposes the caching resolver for in-process callers.
func (s *Service) Resolver() *Resolver { return s.resolver }

func (s *Service) Embed(ctx context.Context, texts []string) ([]Vector, int, error) {
	return s.resolver.Vectors(ctx, texts)
}

func (s *Service) Pool(
	ctx context.Context,
	texts []string,
	weights []float64,
) (Vector, int, error) {
	return s.resolver.Pool(ctx, texts, weights)
}

func (s *Service) Match(centroid Vector, k int) ([]Match, error) {
	return s.index.Search(centroid, k)
}

func (s *Service) Upsert(entity string, centroid Vector, names []NamedVec) error {
	if len(centroid) > 0 {
		err := s.index.Upsert(entity, centroid)
		if err != nil {
			return err
		}
	}

	for _, name := range names {
		err := s.index.AddName(entity, name.Key, name.Vector)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) NearestName(entity string, query Vector) (float64, bool) {
	return s.index.NearestName(entity, query, 0)
}

// SignedEdge is one entity↔entity same-identity edge weight produced by idDiff
// in entity-entity (repair) mode: positive Weight is correlation, negative is
// anti-correlation, Veto is a hard contradiction.
type SignedEdge struct {
	A, B   string
	Weight float64
	Veto   bool
}

// ScoreEdges is the DEFERRED seam the global correlation-clustering REPAIR pass
// consumes: it scores an entity against its blocked neighbours in entity-entity
// mode (disjoint-set negative evidence enabled), without acting on the result.
// Resolve stays greedy; a downstream pass calls this across neighbourhoods and
// re-partitions by reassigning immutable records.
func (s *Service) ScoreEdges(
	ctx context.Context,
	entity string,
	topN int,
	threshold, nameThreshold float64,
) ([]SignedEdge, error) {
	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	centroid, _, err := s.resolver.Pool(ctx, s.entityValues(entity), nil)
	if err != nil {
		return nil, err
	}

	candidates, err := s.index.Search(centroid, topN+1)
	if err != nil {
		return nil, err
	}

	params := s.idDiffParams(threshold, nameThreshold)
	params.ObservationMode = false // entity↔entity: non-overlap is real evidence

	subject := s.entityScoredClaims(entity)
	edges := make([]SignedEdge, 0, len(candidates))

	for _, candidate := range candidates {
		if candidate.Entity == entity {
			continue
		}

		result := idDiff(subject, s.entityScoredClaims(candidate.Entity), params)
		edges = append(edges, SignedEdge{
			A: entity, B: candidate.Entity, Weight: result.Score, Veto: result.Veto,
		})
	}

	return edges, nil
}

func (s *Service) Snapshot() ([]byte, error) {
	return s.index.Snapshot()
}

// ServiceStatus reports cache/index sizes plus cumulative cache-hit telemetry.
type ServiceStatus struct {
	Model        string
	CachedClaims int
	Entities     int
	Lookups      int64
	Misses       int64
}

// Status reports cache + index sizes, the configured model, and the cumulative
// cache lookup/miss counters (hit rate = 1 - Misses/Lookups, should -> 1.0).
func (s *Service) Status() ServiceStatus {
	cached := 0

	var lookups, misses int64

	if s.resolver != nil {
		if s.resolver.cache != nil {
			cached = s.resolver.cache.Len()
		}

		lookups, misses = s.resolver.Stats()
	}

	return ServiceStatus{
		Model:        s.model,
		CachedClaims: cached,
		Entities:     s.index.Len(),
		Lookups:      lookups,
		Misses:       misses,
	}
}
