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
	// resolveMu serializes Resolve so the match->mint->diff->upsert read-modify-
	// write of the index+ledger is atomic (import is sequential; concurrent gRPC
	// Resolve calls thus queue rather than racing the entity space).
	resolveMu sync.Mutex
}

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
		resolver: resolver,
		index:    index,
		ledger:   newEntityLedger(),
		newID:    defaultIDSource(),
		model:    model,
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
		if err := s.index.Upsert(entity, centroid); err != nil {
			return err
		}
	}
	for _, name := range names {
		if err := s.index.AddName(entity, name.Key, name.Vector); err != nil {
			return err
		}
	}

	return nil
}

func (s *Service) NearestName(entity string, query Vector) (float64, bool) {
	return s.index.NearestName(entity, query, 0)
}

func (s *Service) Snapshot() ([]byte, error) {
	return s.index.Snapshot()
}

// Status reports cache + index sizes and the configured model.
func (s *Service) Status() (cachedClaims, entities int, model string) {
	cached := 0
	if s.resolver != nil && s.resolver.cache != nil {
		cached = s.resolver.cache.Len()
	}

	return cached, s.index.Len(), s.model
}
