// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"math"
	"sort"
	"strings"

	"github.com/oklog/ulid/v2"

	"blackcat.ca/gmeow/internal/ontology"
)

// ClaimInput is one claim presented to resolution: its identity-bearing text
// (embedded + grouped), a stable subject-independent hash (for the delta diff),
// and whether it is a name claim (transitional; kind is now derived from the
// attribute — see claim.go). Validity/source/supersedes ride as RDF* annotations
// on the persisted record and are not yet threaded over the wire (the temporal
// and supersedes gates degrade to no-ops until they are — see
// docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §5).
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

// entityLedger is the materialized per-entity claim set (hash -> text) plus a
// corpus document-frequency index (value hash -> # of distinct entities holding
// it) used for ω/IDF weighting. Rebuildable from the record log, so it is a
// cache, not source truth; df is recomputed on load.
type entityLedger struct {
	claims map[string]map[string]string
	df     map[string]int
}

func newEntityLedger() *entityLedger {
	return &entityLedger{
		claims: make(map[string]map[string]string),
		df:     make(map[string]int),
	}
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
		if _, existed := set[claim.Hash]; !existed {
			l.df[claim.Hash]++ // a new (entity, value) pair raises document frequency
		}

		set[claim.Hash] = claim.Text
	}
}

// docFreq is the number of distinct entities asserting a value (its corpus
// document frequency), the denominator of IDF.
func (l *entityLedger) docFreq(hash string) int { return l.df[hash] }

// rebuildDF recomputes the document-frequency index from the claim sets. Called
// after a bulk load where df was not serialized.
func (l *entityLedger) rebuildDF() {
	l.df = make(map[string]int)
	for _, set := range l.claims {
		for hash := range set {
			l.df[hash]++
		}
	}
}

// sortedClaimTexts returns the entity's claim texts in a stable order (sorted by
// hash) so derived centroids/value lists are deterministic.
func (l *entityLedger) sortedClaimTexts(entity string) []string {
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
// claims using the idDiff model (docs/architecture/CONTACT_IDENTITY_RESOLUTION.md
// §4.1): embed the claim values, BLOCK candidate entities with the HNSW index
// (centroid is a recall key only, no longer the decision), then score each
// candidate with the signed, weighted idDiff and merge into the best one that
// clears the gate without an IAC veto — else mint a new ULID. Unchanged: the
// ledger diff → NOOP, and persisting the new claims as an immutable delta record.
func (s *Service) Resolve(
	ctx context.Context,
	claims []ClaimInput,
	threshold, nameThreshold float64,
) (Resolution, error) {
	if len(claims) == 0 {
		return Resolution{IsNoop: true}, nil
	}

	s.resolveMu.Lock()
	defer s.resolveMu.Unlock()

	// Idempotency short-circuit: an IDENTICAL observation (same claim set) always
	// resolves to the same entity as a NOOP. This makes resolution deterministic
	// regardless of centroid drift or blocking recall, so a re-ingest never mints
	// a duplicate (the entity-resolution counterpart of the FILESTORE source
	// dedup; see docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §5).
	obs := observationKey(claims)
	if entity, ok := s.seenObs[obs]; ok {
		return Resolution{Entity: entity, IsNoop: true}, nil
	}

	// Embed the VALUE alone (not "attr: value"): idDiff compares values within an
	// attribute, so the predicate prefix would only inflate cosine between two
	// distinct identifiers (every "email: …" shares the prefix). The claim-vector
	// cache is thus keyed by StatementHash(value).
	attrs := make([]string, len(claims))
	values := make([]string, len(claims))
	for i, claim := range claims {
		attrs[i], values[i] = splitClaimText(claim.Text)
	}

	vectors, _, err := s.resolver.Vectors(ctx, values)
	if err != nil {
		return Resolution{}, err
	}

	incoming := make([]scoredClaim, len(claims))
	for i, claim := range claims {
		concept := attrs[i] // already the canonical concept (claim_extract grounds on ontology)
		incoming[i] = scoredClaim{
			Attr:  concept,
			Value: values[i],
			Hash:  claim.Hash,
			Kind:  ontology.KindForConcept(concept),
			Vec:   vectors[i],
		}
	}

	entity, similarity, isNew, err := s.resolveEntity(
		incoming,
		vectors,
		threshold,
		nameThreshold,
	)
	if err != nil {
		return Resolution{}, err
	}

	s.seenObs[obs] = entity // memoize the decision for identical re-ingests

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
	delete(s.entityClaims, entity) // entity changed: drop its memoized scored claims

	// Recall centroid = mean of the entity's claim VALUE vectors (cache hits). It
	// is only an HNSW blocking key now; idDiff makes the decision, so its drift no
	// longer corrupts matching.
	newCentroid, _, err := s.resolver.Pool(ctx, s.entityValues(entity), nil)
	if err != nil {
		return Resolution{}, err
	}

	if err := s.Upsert(entity, newCentroid, nil); err != nil {
		return Resolution{}, err
	}

	return Resolution{
		Entity:         entity,
		NewClaimHashes: newHashes,
		Similarity:     similarity,
		IsNew:          isNew,
	}, nil
}

// resolveEntity blocks candidate entities via the HNSW index, scores each with
// idDiff, and returns the best entity clearing the merge gate (or a freshly
// minted ULID). The greedy single-assignment here is the ingest path; the
// deferred global partition (ScoreEdges) re-clusters downstream.
func (s *Service) resolveEntity(
	incoming []scoredClaim,
	vectors []Vector,
	threshold, nameThreshold float64,
) (entity string, similarity float64, isNew bool, err error) {
	centroid, err := MeanPool(vectors, nil)
	if err != nil {
		return "", 0, false, err
	}

	candidates, err := s.index.Search(centroid, s.blockingTopN)
	if err != nil {
		return "", 0, false, err
	}

	params := s.idDiffParams(threshold, nameThreshold)

	bestEntity := ""
	bestScore := s.mergeGate // must clear the gate to win
	bestConfidence := 0.0

	for _, candidate := range candidates {
		result := idDiff(incoming, s.entityScoredClaims(candidate.Entity), params)
		if result.Veto {
			continue
		}

		if result.Score >= bestScore {
			bestEntity = candidate.Entity
			bestScore = result.Score
			bestConfidence = result.Confidence
		}
	}

	if bestEntity != "" {
		return bestEntity, bestConfidence, false, nil
	}

	return s.newID(), 0, true, nil
}

// idDiffParams builds the per-call comparison parameters. The CLI's
// match-threshold becomes the set/identifier value-match cosine; name-threshold
// becomes the (stricter) functional value-match cosine — so name variants still
// match but distinct names contradict.
func (s *Service) idDiffParams(threshold, nameThreshold float64) idDiffParams {
	entities := s.index.Len()
	docFreq := s.ledger.docFreq

	return idDiffParams{
		TauSet:          threshold,
		TauFunc:         math.Max(nameThreshold, threshold),
		TauCtx:          s.tauCtx,
		Lambda:          s.lambda,
		SetPenalty:      s.setPenalty,
		VetoMass:        s.vetoMass,
		ObservationMode: true,
		W: func(c scoredClaim) float64 {
			return omega(c.Kind, docFreq(c.Hash), entities)
		},
	}
}

// entityScoredClaims materializes an entity's folded claim set as structured
// scoredClaims (canonical attribute, value vector hydrated from the cache). The
// result is MEMOIZED per entity and invalidated only when the entity gains a
// claim (see Resolve), so blocking candidates that are unchanged between Resolve
// calls are not re-hydrated — this keeps idDiff re-ranking cheap at scale.
func (s *Service) entityScoredClaims(entity string) []scoredClaim {
	if cached, ok := s.entityClaims[entity]; ok {
		return cached
	}

	set := s.ledger.claims[entity]
	out := make([]scoredClaim, 0, len(set))

	for hash, text := range set {
		concept, value := splitClaimText(text)
		claim := scoredClaim{
			Attr:  concept,
			Value: value,
			Hash:  hash,
			Kind:  ontology.KindForConcept(concept),
		}
		if vec, ok := s.resolver.Cache().Get(StatementHash(value)); ok {
			claim.Vec = vec
		}

		out = append(out, claim)
	}

	s.entityClaims[entity] = out

	return out
}

// entityValues returns an entity's claim VALUES (the value-only embed inputs) in
// a stable order, for the recall centroid.
func (s *Service) entityValues(entity string) []string {
	texts := s.ledger.sortedClaimTexts(entity)
	values := make([]string, len(texts))

	for i, text := range texts {
		_, values[i] = splitClaimText(text)
	}

	return values
}

// observationKey is the stable identity of an observation's claim set: a hash of
// its sorted claim hashes. Identical claim sets share a key (and thus a memoized
// entity), so a re-ingest is a deterministic NOOP.
func observationKey(claims []ClaimInput) string {
	hashes := make([]string, len(claims))
	for i, claim := range claims {
		hashes[i] = claim.Hash
	}

	sort.Strings(hashes)

	return StatementHash(strings.Join(hashes, "\n"))
}

func defaultIDSource() func() string {
	return func() string { return ulid.Make().String() }
}
