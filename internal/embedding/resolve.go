// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"sort"

	"github.com/oklog/ulid/v2"
)

// ClaimInput is one claim presented to resolution: its identity-bearing text
// (embedded + pooled), a stable subject-independent hash (for the delta diff),
// and whether it is a name claim (name claims seed the entity's name-vector set).
type ClaimInput struct {
	Text   string
	Hash   string
	IsName bool
}

// Resolution is the outcome of resolving one record's claims against the entity
// space: the chosen entity ULID, whether it was freshly minted, whether the
// record carried no new information (NOOP), and the hashes of the claims that
// were new (which the caller persists as an immutable delta record).
type Resolution struct {
	Entity         string
	NewClaimHashes []string
	Similarity     float64
	IsNew          bool
	IsNoop         bool
}

// entityLedger is the materialized per-entity claim set (hash -> text), used to
// diff incoming claims and re-pool the centroid. Rebuildable from the record
// log, so it is a cache, not source truth.
type entityLedger struct {
	claims map[string]map[string]string
}

func newEntityLedger() *entityLedger {
	return &entityLedger{claims: make(map[string]map[string]string)}
}

func (l *entityLedger) has(entity, hash string) bool {
	_, ok := l.claims[entity][hash]

	return ok
}

func (l *entityLedger) add(entity string, claims []ClaimInput) {
	set := l.claims[entity]
	if set == nil {
		set = make(map[string]string)
		l.claims[entity] = set
	}

	for _, claim := range claims {
		set[claim.Hash] = claim.Text
	}
}

// texts returns the entity's full claim-text set in a stable order (sorted by
// hash) so the re-pooled centroid is deterministic.
func (l *entityLedger) texts(entity string) []string {
	set := l.claims[entity]

	hashes := make([]string, 0, len(set))
	for hash := range set {
		hashes = append(hashes, hash)
	}

	sort.Strings(hashes)

	texts := make([]string, len(hashes))
	for i, hash := range hashes {
		texts[i] = set[hash]
	}

	return texts
}

// Resolve runs the ingest-time match→delta→NOOP decision for one record's
// claims: pool them into a candidate centroid, match against existing entity
// centroids (>= threshold matches; else mint a ULID), diff against the entity's
// ledger, and — unless it is a NOOP — append the new claims to the ledger and
// re-pool the centroid (plus any new name vectors) in the index. The caller
// persists the returned NewClaimHashes as an immutable delta record.
func (s *Service) Resolve(
	ctx context.Context,
	claims []ClaimInput,
	threshold, nameThreshold float64,
) (Resolution, error) {
	if len(claims) == 0 {
		return Resolution{IsNoop: true}, nil
	}

	if nameThreshold < threshold {
		nameThreshold = threshold // names must agree at least as strongly as centroids
	}

	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	texts := make([]string, len(claims))
	for i, claim := range claims {
		texts[i] = claim.Text
	}

	centroid, _, err := s.resolver.Pool(ctx, texts, nil)
	if err != nil {
		return Resolution{}, err
	}

	entity := ""
	similarity := 0.0
	isNew := false

	matches, err := s.index.Search(centroid, 1)
	if err != nil {
		return Resolution{}, err
	}

	if len(matches) > 0 && matches[0].Similarity >= threshold {
		// A centroid match must also AGREE ON NAME when both sides carry one:
		// related-but-distinct entities (a person and their org) share enough
		// context to exceed the centroid threshold, but their names diverge. The
		// name-vector layer rejects those false merges. Absent names on either
		// side, the centroid match stands.
		agrees, nameErr := s.nameAgrees(ctx, matches[0].Entity, claims, nameThreshold)
		if nameErr != nil {
			return Resolution{}, nameErr
		}

		if agrees {
			entity = matches[0].Entity
			similarity = matches[0].Similarity
		}
	}

	if entity == "" {
		entity = s.newID()
		isNew = true
	}

	var (
		newClaims []ClaimInput
		newHashes []string
	)

	for _, claim := range claims {
		if !s.ledger.has(entity, claim.Hash) {
			newClaims = append(newClaims, claim)
			newHashes = append(newHashes, claim.Hash)
		}
	}

	if !isNew && len(newClaims) == 0 {
		return Resolution{Entity: entity, Similarity: similarity, IsNoop: true}, nil
	}

	s.ledger.add(entity, newClaims)

	newCentroid, _, err := s.resolver.Pool(ctx, s.ledger.texts(entity), nil)
	if err != nil {
		return Resolution{}, err
	}

	names, err := s.nameVectors(ctx, entity, newClaims)
	if err != nil {
		return Resolution{}, err
	}

	if err := s.Upsert(entity, newCentroid, names); err != nil {
		return Resolution{}, err
	}

	return Resolution{
		Entity:         entity,
		NewClaimHashes: newHashes,
		Similarity:     similarity,
		IsNew:          isNew,
	}, nil
}

// nameAgrees reports whether the incoming record's name(s) are consistent with
// the candidate entity's name-vector set. It returns true when either side has
// no name to compare (the centroid match then stands), and otherwise requires
// the best incoming-vs-entity name similarity to meet the threshold. This is the
// guard that keeps a person and their tightly-coupled org from merging on
// centroid overlap alone.
func (s *Service) nameAgrees(
	ctx context.Context,
	entity string,
	claims []ClaimInput,
	threshold float64,
) (bool, error) {
	var nameTexts []string

	for _, claim := range claims {
		if claim.IsName {
			nameTexts = append(nameTexts, claim.Text)
		}
	}

	if len(nameTexts) == 0 {
		return true, nil
	}

	vectors, _, err := s.Embed(ctx, nameTexts)
	if err != nil {
		return false, err
	}

	sawEntityName := false
	best := -2.0

	for _, vector := range vectors {
		sim, found := s.index.NearestName(entity, vector, 0)
		if !found {
			continue // the entity has no name vectors yet; cannot disconfirm
		}

		sawEntityName = true

		if sim > best {
			best = sim
		}
	}

	if !sawEntityName {
		return true, nil
	}

	return best >= threshold, nil
}

// nameVectors embeds the name claims among the delta and returns them as entity
// name vectors (keyed entity+hash). Renames accumulate name vectors per entity.
func (s *Service) nameVectors(
	ctx context.Context,
	entity string,
	delta []ClaimInput,
) ([]NamedVec, error) {
	var texts, keys []string

	for _, claim := range delta {
		if !claim.IsName {
			continue
		}

		texts = append(texts, claim.Text)
		keys = append(keys, entity+":"+claim.Hash)
	}

	if len(texts) == 0 {
		return nil, nil
	}

	vectors, _, err := s.Embed(ctx, texts)
	if err != nil {
		return nil, err
	}

	names := make([]NamedVec, len(vectors))
	for i, vector := range vectors {
		names[i] = NamedVec{Key: keys[i], Vector: vector}
	}

	return names, nil
}

func defaultIDSource() func() string {
	return func() string { return ulid.Make().String() }
}
