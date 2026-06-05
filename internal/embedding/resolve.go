// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"context"
	"math"
	"sort"
	"strings"
	"time"

	"github.com/oklog/ulid/v2"

	"blackcat.ca/gmeow/internal/ontology"
)

// ClaimInput is one claim presented to resolution: its identity-bearing text
// (embedded + grouped), a stable subject-independent hash (for the delta diff),
// and whether it is a name claim (transitional; kind is now derived from the
// attribute — see claim.go). ValidFrom/ValidUntil are the VALID-time (tenure)
// bounds — RFC3339, empty = unbounded — the ONLY clock that feeds the co-validity
// gate (assertion/carrier/transaction time never reach here; see
// docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4.1 and the four-clock model
// in ~/Active/gmeow-ontology/docs/import-provenance.md). Source/supersedes still
// ride as RDF* annotations on the persisted record.
type ClaimInput struct {
	Text       string
	Hash       string
	IsName     bool
	ValidFrom  string
	ValidUntil string
}

// parseValidity builds a validity Interval from RFC3339 ValidFrom/ValidUntil
// strings; an empty or unparseable bound is left unbounded on that side (the
// graceful default — a claim the source did not temporally ground stays null).
func parseValidity(from, until string) Interval {
	parse := func(s string) time.Time {
		if s == "" {
			return time.Time{}
		}
		if t, err := time.Parse(time.RFC3339, s); err == nil {
			return t
		}

		return time.Time{}
	}

	return Interval{First: parse(from), Last: parse(until)}
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

// identifierConcepts are the high-discrimination identifier concepts whose values
// seed the inverted blocking index: a shared one is a strong reason to RETRIEVE a
// candidate (idDiff/ω then decide whether to actually merge). Names/org/etc. are
// deliberately excluded — too common, they would widen false candidates.
var identifierConcepts = map[string]bool{
	"email": true, "phone": true, "account": true, "url": true,
}

// entityLedger is the materialized per-entity claim set (hash -> text) plus two
// derived indexes: df (value hash -> # of distinct entities, for ω/IDF) and ident
// (identifier value -> entities, for value-based blocking). Both are rebuildable
// from the claim sets — the ledger is the source of record, the indexes are views
// recomputed on load (never serialized).
type entityLedger struct {
	claims map[string]map[string]claimEntry
	df     map[string]int
	ident  map[string][]string
}

// claimEntry is an entity's folded view of one claim: its text plus the VALID-time
// (tenure) bounds (RFC3339, empty = unbounded — the four-clock valid clock). The
// candidate side of the co-validity gate reads these, so they must survive folds
// and the state snapshot.
type claimEntry struct {
	Text       string
	ValidFrom  string
	ValidUntil string
}

func newEntityLedger() *entityLedger {
	return &entityLedger{
		claims: make(map[string]map[string]claimEntry),
		df:     make(map[string]int),
		ident:  make(map[string][]string),
	}
}

func (l *entityLedger) has(entity, hash string) bool {
	_, ok := l.claims[entity][hash]

	return ok
}

func (l *entityLedger) add(entity string, claims []ClaimInput) {
	set := l.claims[entity]
	if set == nil {
		set = make(map[string]claimEntry)
		l.claims[entity] = set
	}

	for _, claim := range claims {
		existing, existed := set[claim.Hash]
		if !existed {
			l.df[claim.Hash]++ // a new (entity, value) pair raises document frequency
			l.indexIdentifier(entity, claim.Text)
			set[claim.Hash] = claimEntry{
				Text: claim.Text, ValidFrom: claim.ValidFrom, ValidUntil: claim.ValidUntil,
			}

			continue
		}
		// Same value re-observed: widen the held interval to the union span (the
		// entity held the value across all observed validity windows).
		existing.ValidFrom = earlierBound(existing.ValidFrom, claim.ValidFrom)
		existing.ValidUntil = laterBound(existing.ValidUntil, claim.ValidUntil)
		set[claim.Hash] = existing
	}
}

// earlierBound / laterBound widen a validity span across re-observations. An empty
// bound is unbounded (extends to infinity), so it absorbs any value.
func earlierBound(a, b string) string {
	if a == "" || b == "" {
		return ""
	}
	if b < a { // RFC3339 sorts lexicographically by time
		return b
	}

	return a
}

func laterBound(a, b string) string {
	if a == "" || b == "" {
		return ""
	}
	if b > a {
		return b
	}

	return a
}

// indexIdentifier adds an entity to the inverted blocking index under an
// identifier claim's value (no-op for non-identifier concepts).
func (l *entityLedger) indexIdentifier(entity, text string) {
	concept, value := splitClaimText(text)
	if value == "" || !identifierConcepts[concept] {
		return
	}

	key := concept + "\x00" + value
	l.ident[key] = append(l.ident[key], entity)
}

// identifierCandidates returns the entities sharing any identifier value with the
// incoming claims — the value-based blocking candidates (deduped, sorted for
// deterministic resolution).
func (l *entityLedger) identifierCandidates(claims []scoredClaim) []string {
	seen := map[string]bool{}
	out := []string{}

	for _, claim := range claims {
		if !identifierConcepts[claim.Attr] || claim.Value == "" {
			continue
		}
		for _, entity := range l.ident[claim.Attr+"\x00"+claim.Value] {
			if !seen[entity] {
				seen[entity] = true
				out = append(out, entity)
			}
		}
	}

	sort.Strings(out)

	return out
}

// docFreq is the number of distinct entities asserting a value (its corpus
// document frequency), the denominator of IDF.
func (l *entityLedger) docFreq(hash string) int { return l.df[hash] }

// rebuild recomputes the derived df and identifier indexes from the claim sets.
// Called after a bulk load (neither index is serialized).
func (l *entityLedger) rebuild() {
	l.df = make(map[string]int)
	l.ident = make(map[string][]string)
	for entity, set := range l.claims {
		for hash, entry := range set {
			l.df[hash]++
			l.indexIdentifier(entity, entry.Text)
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
		texts[i] = set[hash].Text
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

	// Refresh the residual basis as the value cache grows, so idDiff compares in
	// residual space (shared-structure variance removed) rather than raw cosine.
	s.maybeRebuildResidual()

	incoming := make([]scoredClaim, len(claims))
	for i, claim := range claims {
		concept := attrs[i] // already the canonical concept (claim_extract grounds on ontology)
		incoming[i] = scoredClaim{
			Attr:  concept,
			Value: values[i],
			Hash:  claim.Hash,
			Kind:  ontology.KindForConcept(concept),
			Vec:   s.residualize(vectors[i]),
			Valid: parseValidity(claim.ValidFrom, claim.ValidUntil),
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

	// Blocking is the UNION of two recall channels: the centroid HNSW (gestalt
	// similarity) and the inverted identifier index (a shared email/phone/account/
	// url retrieves the candidate regardless of centroid drift — the cross-format
	// recall the centroid alone misses). idDiff then decides which actually merge.
	candidates, err := s.index.Search(centroid, s.blockingTopN)
	if err != nil {
		return "", 0, false, err
	}

	candidateEntities := map[string]bool{}
	ordered := []string{}
	addCandidate := func(entity string) {
		if entity != "" && !candidateEntities[entity] {
			candidateEntities[entity] = true
			ordered = append(ordered, entity)
		}
	}
	for _, candidate := range candidates {
		addCandidate(candidate.Entity)
	}
	for _, entity := range s.ledger.identifierCandidates(incoming) {
		addCandidate(entity)
	}
	sort.Strings(ordered) // deterministic scoring order (tie-break by ULID)

	params := s.idDiffParams(threshold, nameThreshold)

	bestEntity := ""
	bestScore := s.mergeGate // must clear the gate to win
	bestConfidence := 0.0

	for _, entity := range ordered {
		result := idDiff(incoming, s.entityScoredClaims(entity), params)
		if result.Veto {
			continue
		}

		if result.Score >= bestScore {
			bestEntity = entity
			bestScore = result.Score
			bestConfidence = result.Confidence
		}
	}

	if bestEntity != "" {
		return bestEntity, bestConfidence, false, nil
	}

	return s.newID(), 0, true, nil
}

// maybeRebuildResidual rebuilds the value-embedding residual basis when the cache
// has grown enough since the last build (geometric: first at residualMinSample,
// then on each doubling). The caller holds resolveMu. A rebuild invalidates the
// memoized per-entity scored claims, whose residual vectors depend on the basis.
func (s *Service) maybeRebuildResidual() {
	if s.residualK <= 0 {
		return
	}

	n := s.resolver.Cache().Len()
	if n < s.residualMinSample {
		return
	}
	if s.residual != nil && n < 2*s.residualBuiltAt {
		return
	}

	basis := computeResidualBasis(s.cacheEntries(), s.residualK)
	if basis == nil {
		return
	}

	s.residual = basis
	s.residualBuiltAt = n
	s.entityClaims = make(map[string][]scoredClaim) // residuals changed: drop the memo
}

// residualize projects shared-structure variance out of a value embedding before it
// enters idDiff (residual.go). A no-op until a basis exists. Blocking/centroid keys
// stay RAW (recall); only the idDiff comparison uses residual space (precision).
func (s *Service) residualize(v Vector) Vector {
	return s.residual.residual(v)
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
		TauName:         math.Max(nameThreshold, threshold),
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

	for hash, entry := range set {
		concept, value := splitClaimText(entry.Text)
		claim := scoredClaim{
			Attr:  concept,
			Value: value,
			Hash:  hash,
			Kind:  ontology.KindForConcept(concept),
			Valid: parseValidity(entry.ValidFrom, entry.ValidUntil),
		}
		if vec, ok := s.resolver.Cache().Get(StatementHash(value)); ok {
			claim.Vec = s.residualize(
				vec,
			) // compare in residual space (memo dropped on basis rebuild)
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
	// Include valid-time bounds: the same value asserted with different validity is
	// a DIFFERENT observation (a transfer, not a re-ingest), so it must not collapse
	// to the same memo entry and short-circuit the co-validity gate.
	keys := make([]string, len(claims))
	for i, claim := range claims {
		keys[i] = claim.Hash + "\x00" + claim.ValidFrom + "\x00" + claim.ValidUntil
	}

	sort.Strings(keys)

	return StatementHash(strings.Join(keys, "\n"))
}

func defaultIDSource() func() string {
	return func() string { return ulid.Make().String() }
}
