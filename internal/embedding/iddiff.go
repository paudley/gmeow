// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"math"
	"slices"
	"sort"

	"blackcat.ca/gmeow/internal/ontology"
)

// idDiffParams are the tunables of the signed comparison (see CONTACT_IDENTITY_
// RESOLUTION.md §4.1). They live on the Service and are passed per call.
type idDiffParams struct {
	// TauSet/TauFunc/TauCtx are the cosine value-match thresholds per kind —
	// identifiers must match near-exactly, names a touch looser (variants), free
	// context loosest.
	TauSet  float64
	TauFunc float64
	TauCtx  float64
	// Lambda weights IAC (anti-correlation) against IC in the signed score.
	Lambda float64
	// SetPenalty scales the "expected-but-absent overlap" IAC for two populated,
	// disjoint sets (entity↔entity comparisons only — see ObservationMode).
	SetPenalty float64
	// VetoMass: a single functional contradiction of at least this ω hard-vetoes
	// the merge regardless of accumulated IC.
	VetoMass float64
	// TauName is the per-token cosine match threshold for name tokens (high — a
	// name token matches its near-exact spelling/typo, not a semantic neighbour).
	TauName float64
	// ObservationMode suppresses set-disjoint negative evidence: at ingest the A
	// side is a PARTIAL observation (it simply may not repeat the entity's other
	// identifiers), so non-overlap is neutral, not contradictory. The global
	// repair pass compares two folded entities with ObservationMode=false.
	ObservationMode bool
	// W returns a claim's identifying weight ω.
	W func(scoredClaim) float64
}

// idDiffResult is one signed comparison: the raw signed score, its IC/IAC
// components (telemetry), and whether a hard veto fired.
type idDiffResult struct {
	Score      float64
	Confidence float64
	IC         float64
	IAC        float64
	Veto       bool
}

// idDiff compares two claim graphs and returns signed evidence that they denote
// the same identity. A is the incoming/query graph, B the candidate. The result
// is the edge weight the resolver (greedy) or the deferred global partition
// consumes.
func idDiff(a, b []scoredClaim, p idDiffParams) idDiffResult {
	ag := groupByAttr(frontier(a))
	bg := groupByAttr(frontier(b))

	var ic, iac, anchorIC float64
	var setICRaw, setIncoming, anchorSetICRaw float64

	veto := false

	for _, attr := range unionAttrs(ag, bg) {
		av := dedupBySource(ag[attr])
		bv := dedupBySource(bg[attr])
		if len(av) == 0 || len(bv) == 0 {
			continue // ISD: present on one side only, no expectation — neutral
		}

		switch kindOf(av, bv) {
		case KindSet:
			shared := setOverlapMass(av, bv, p)
			setIncoming += shared.Incoming
			if shared.Raw > 0 {
				setICRaw += shared.Raw
				if identityAnchor(av, bv) {
					anchorSetICRaw += shared.Raw
				}
			} else if !p.ObservationMode && len(av) >= 2 && len(bv) >= 2 {
				// Two populated, disjoint identifier sets => negative (set identity).
				iac += p.SetPenalty * minMass(av, bv, p.W)
			}
		case KindFunctional:
			fic, fiac, fveto := functionalScore(av, bv, p)
			ic += fic
			iac += fiac
			anchorIC += fic
			if fveto {
				veto = true
			}
		case KindName:
			nic, niac := nameScore(av, bv, p)
			ic += nic
			iac += niac
			anchorIC += nic
		default: // KindContextual
			ic += contextualMass(av, bv, p)
		}
	}

	if iac > 0 {
		setIC := proportionalMass(setICRaw, setIncoming)
		ic += setIC
		if setICRaw > 0 {
			anchorIC += anchorSetICRaw * (setIC / setICRaw)
		}
	} else {
		ic += setICRaw
		anchorIC += anchorSetICRaw
	}

	score := ic - p.Lambda*iac
	if p.ObservationMode && anchorIC == 0 {
		score = 0
	}

	return idDiffResult{
		Score:      score,
		Confidence: sigmoid(score),
		IC:         ic,
		IAC:        iac,
		Veto:       veto,
	}
}

// identityAnchor reports whether positive overlap on this attribute is direct
// identity evidence during greedy ingest. Workplace, relationship, notes, and
// address-part context can corroborate an anchored match, but they do not by
// themselves identify a person: many people legitimately share them.
func identityAnchor(av, bv []scoredClaim) bool {
	kind := kindOf(av, bv)
	if kind == KindName || kind == KindFunctional {
		return true
	}
	if kind != KindSet {
		return false
	}

	role := roleOf(av, bv)
	switch role {
	case ontology.RoleAccount, ontology.RoleIdentifier:
		return true
	case ontology.RoleLocator:
		switch attrOf(av, bv) {
		case "email", "phone", "url":
			return true
		}
	}

	return false
}

type setOverlapResult struct {
	Raw      float64
	Incoming float64
}

// setOverlapMass scores shared identifier values by proportional identifying
// mass. Each INCOMING claim contributes at most its single best match (1:1), so
// mass reflects the newcomer's own evidence rather than the candidate's breadth;
// then the shared mass is scaled by its share of the incoming set. This preserves
// strong evidence for complete overlap while making "2 of 45 emails" weak
// without introducing a cap or threshold.
func setOverlapMass(av, bv []scoredClaim, p idDiffParams) setOverlapResult {
	var shared float64
	var incoming float64

	for _, ca := range av {
		incoming += p.W(ca)
		var best float64
		for _, cb := range bv {
			if valueMatch(ca, cb, p.TauSet) {
				if m := math.Min(p.W(ca), p.W(cb)); m > best {
					best = m
				}
			}
		}
		shared += best
	}
	if incoming == 0 {
		return setOverlapResult{}
	}

	return setOverlapResult{
		Raw:      shared,
		Incoming: incoming,
	}
}

func proportionalMass(shared, incoming float64) float64 {
	if incoming == 0 {
		return 0
	}

	return shared * (shared / incoming)
}

// functionalScore scores a single-valued attribute: a co-valid value-match is
// IC; a co-valid, unsuperseded mismatch is a contradiction (IAC, and a veto when
// heavy); a supersedes-linked mismatch is a licensed transition (neither).
func functionalScore(
	av, bv []scoredClaim,
	p idDiffParams,
) (ic, iac float64, veto bool) {
	for _, ca := range av {
		for _, cb := range bv {
			if !ca.Valid.Overlaps(cb.Valid) {
				continue
			}

			switch {
			case valueMatch(ca, cb, p.TauFunc):
				ic += p.W(ca)
			case linkedBySupersedes(ca, cb):
				// evolution edge: neither IC nor IAC.
			default:
				mass := math.Max(p.W(ca), p.W(cb))
				iac += mass
				if mass >= p.VetoMass {
					veto = true
				}
			}
		}
	}

	return ic, iac, veto
}

// nameScore compares two sets of personal-name TOKENS by role-free SUBSUMPTION
// (CONTACT_IDENTITY_RESOLUTION.md §4.2). There is no privileged "surname"; the
// STRUCTURE of the token sets, not a role, decides:
//
//   - SUBSET / completion — one side's substantive tokens are all matched ("Patrick"
//     ⊂ "Patrick Audley"; "Patrick Audley" + a middle "Colm"): the shared tokens
//     CORROBORATE, weighted by rarity (ω). A rare shared token ("Audley") is strong;
//     a common one ("Patrick") weak. No penalty.
//   - DIVERGENCE — BOTH sides carry a substantive (non-initial) UNMATCHED token
//     ("Patrick Audley" vs "Patrick Smith"; "Patrick Audley" vs "Susan Audley"):
//     the names denote different people, so the shared tokens are coincidence
//     (a shared family name, a common given name) and DO NOT corroborate — IC is
//     withheld — plus a penalty scaled by the weaker side's distinctive mass.
//
// Withholding IC on divergence (rather than only penalising) is the conservative,
// anti-over-merge choice: a shared rare surname must not by itself fuse two
// clearly-different full names. A supersedes link across the sets licenses a
// divergence (a name change) with neither effect. Initials match loosely and never
// count as divergence (an abbreviation that simply failed to expand).
func nameScore(av, bv []scoredClaim, p idDiffParams) (ic, iac float64) {
	matchedB := make([]bool, len(bv))
	matchedA := make([]bool, len(av))

	var shared float64

	for i, ca := range av {
		best := -1

		var bestMass float64

		for j, cb := range bv {
			if matchedB[j] || !ca.Valid.Overlaps(cb.Valid) {
				continue
			}
			if !nameTokenMatch(ca, cb, p.TauName) {
				continue
			}
			if m := math.Min(p.W(ca), p.W(cb)); m > bestMass {
				bestMass = m
				best = j
			}
		}

		if best >= 0 {
			matchedB[best] = true
			matchedA[i] = true
			shared += bestMass
		}
	}

	divA := substantiveUnmatched(av, matchedA)
	divB := substantiveUnmatched(bv, matchedB)
	if divA && divB && !supersedesAcross(av, bv) {
		// Mutual substantive divergence => different people: no corroboration, plus a
		// penalty by the weaker distinctive side (common diverging tokens penalise
		// little; rare ones split decisively).
		return 0, math.Min(
			maxUnmatchedMass(av, matchedA, p),
			maxUnmatchedMass(bv, matchedB, p),
		)
	}

	return shared, 0
}

// substantiveUnmatched reports whether a token set has an UNMATCHED token that is
// not a single-letter initial — the structural test for "this side carries a
// distinctive token the other name lacks" (the divergence half-signal).
func substantiveUnmatched(claims []scoredClaim, matched []bool) bool {
	for i, c := range claims {
		if !matched[i] && len([]rune(c.Value)) > 1 {
			return true
		}
	}

	return false
}

// nameTokenMatch reports whether two name tokens are the same token: exact
// normalized equality, a high-cosine variant (typo/spelling), or an initial
// matching a full token with the same leading rune (e.g. "p" ↔ "patrick").
func nameTokenMatch(a, b scoredClaim, tau float64) bool {
	if a.Value == b.Value {
		return true
	}
	if initialMatch(a.Value, b.Value) {
		return true
	}

	return cosineSimilarity(a.Vec, b.Vec) >= tau
}

// initialMatch reports whether one value is a single rune that is the leading rune
// of the other (an abbreviated initial standing in for a full name token).
func initialMatch(a, b string) bool {
	ra, rb := []rune(a), []rune(b)
	if len(ra) == 1 && len(rb) >= 1 {
		return ra[0] == rb[0]
	}
	if len(rb) == 1 && len(ra) >= 1 {
		return rb[0] == ra[0]
	}

	return false
}

// maxUnmatchedMass is the ω of the most distinctive token in a set that earned no
// match — the side's strongest evidence of a token the other name lacks.
func maxUnmatchedMass(claims []scoredClaim, matched []bool, p idDiffParams) float64 {
	var max float64
	for i, c := range claims {
		if matched[i] {
			continue
		}
		if m := p.W(c); m > max {
			max = m
		}
	}

	return max
}

// supersedesAcross reports whether any claim on one side supersedes one on the
// other — a licensed name change (former → chosen), so a divergence is not a
// contradiction.
func supersedesAcross(av, bv []scoredClaim) bool {
	for _, a := range av {
		for _, b := range bv {
			if linkedBySupersedes(a, b) {
				return true
			}
		}
	}

	return false
}

// contextualMass scores soft (contextual) overlap. Each INCOMING claim contributes
// at most its single best match (1:1) — critical for low-discrimination attributes
// like name parts: without it, one incoming given-name scores against EVERY given-
// name a large candidate holds, so contextual IC grows with candidate size and a
// bag of common first/last names accretes unrelated people into one blob. Bounding
// to the best match makes mass reflect the newcomer's own corroboration.
func contextualMass(av, bv []scoredClaim, p idDiffParams) float64 {
	var mass float64

	for _, ca := range av {
		var best float64
		for _, cb := range bv {
			if !ca.Valid.Overlaps(cb.Valid) {
				continue
			}

			if sim := cosineSimilarity(ca.Vec, cb.Vec); sim >= p.TauCtx {
				if m := p.W(ca) * sim; m > best {
					best = m
				}
			}
		}
		mass += best
	}

	return mass
}

// valueMatch is the two-part equality of §4.1: fuzzy embedding equality AND
// temporal co-validity.
func valueMatch(a, b scoredClaim, tau float64) bool {
	return a.Valid.Overlaps(b.Valid) && cosineSimilarity(a.Vec, b.Vec) >= tau
}

// linkedBySupersedes reports whether either claim supersedes the other (an
// evolution edge across the incoming and candidate graphs).
func linkedBySupersedes(a, b scoredClaim) bool {
	return slices.Contains(a.Supersedes, b.ULID) || slices.Contains(b.Supersedes, a.ULID)
}

// frontier drops claims that are superseded by another claim in the same set,
// returning the current (non-superseded) view.
func frontier(claims []scoredClaim) []scoredClaim {
	superseded := map[string]bool{}
	for _, c := range claims {
		for _, s := range c.Supersedes {
			superseded[s] = true
		}
	}

	if len(superseded) == 0 {
		return claims
	}

	out := make([]scoredClaim, 0, len(claims))
	for _, c := range claims {
		if c.ULID == "" || !superseded[c.ULID] {
			out = append(out, c)
		}
	}

	return out
}

// dedupBySource collapses claims sharing a SourceKey within an attribute to one,
// so repeated observations of the same value from one source do not inflate IC
// (source-independence). Claims with an empty SourceKey are singletons.
func dedupBySource(claims []scoredClaim) []scoredClaim {
	seen := map[string]bool{}
	out := make([]scoredClaim, 0, len(claims))

	for _, c := range claims {
		if c.SourceKey != "" {
			key := c.SourceKey + "\x00" + c.Hash
			if seen[key] {
				continue
			}

			seen[key] = true
		}

		out = append(out, c)
	}

	return out
}

func groupByAttr(claims []scoredClaim) map[string][]scoredClaim {
	out := make(map[string][]scoredClaim)
	for _, c := range claims {
		out[c.Attr] = append(out[c.Attr], c)
	}

	return out
}

func unionAttrs(a, b map[string][]scoredClaim) []string {
	set := map[string]bool{}
	for k := range a {
		set[k] = true
	}

	for k := range b {
		set[k] = true
	}

	attrs := make([]string, 0, len(set))
	for k := range set {
		attrs = append(attrs, k)
	}

	sort.Strings(attrs) // deterministic scoring order

	return attrs
}

// kindOf returns the attribute kind from whichever side has claims (both sides
// share the attr, so their kind agrees).
func kindOf(av, bv []scoredClaim) AttrKind {
	if len(av) > 0 {
		return av[0].Kind
	}

	return bv[0].Kind
}

func roleOf(av, bv []scoredClaim) AttrRole {
	if len(av) > 0 {
		return av[0].Role
	}

	return bv[0].Role
}

func attrOf(av, bv []scoredClaim) string {
	if len(av) > 0 {
		return av[0].Attr
	}

	return bv[0].Attr
}

func minMass(av, bv []scoredClaim, w func(scoredClaim) float64) float64 {
	var ma, mb float64
	for _, c := range av {
		ma += w(c)
	}

	for _, c := range bv {
		mb += w(c)
	}

	return math.Min(ma, mb)
}

func sigmoid(x float64) float64 { return 1.0 / (1.0 + math.Exp(-x)) }
