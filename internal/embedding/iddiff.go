// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"math"
	"slices"
	"sort"
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

	var ic, iac float64

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
			if shared > 0 {
				ic += shared
			} else if !p.ObservationMode && len(av) >= 2 && len(bv) >= 2 {
				// Two populated, disjoint identifier sets => negative (set identity).
				iac += p.SetPenalty * minMass(av, bv, p.W)
			}
		case KindFunctional:
			fic, fiac, fveto := functionalScore(av, bv, p)
			ic += fic
			iac += fiac
			if fveto {
				veto = true
			}
		default: // KindContextual
			ic += contextualMass(av, bv, p)
		}
	}

	score := ic - p.Lambda*iac

	return idDiffResult{
		Score:      score,
		Confidence: sigmoid(score),
		IC:         ic,
		IAC:        iac,
		Veto:       veto,
	}
}

// setOverlapMass scores shared identifier values. Each INCOMING claim contributes
// at most its single best match (1:1), so mass reflects the newcomer's own
// evidence rather than the candidate's breadth — a record cannot earn extra mass
// merely because the candidate is large (the rich-get-richer over-merge: one
// "douglas" must not score against every "douglas" a blob accumulated).
func setOverlapMass(av, bv []scoredClaim, p idDiffParams) float64 {
	var shared float64

	for _, ca := range av {
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

	return shared
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
