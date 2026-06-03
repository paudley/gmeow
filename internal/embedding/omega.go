// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import "math"

// Identifying weight ω (docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4.1:
// ω ≈ IDF × kind × source_trust × temporal). Modelled additively as
//
//	ω = kindBase(kind) + idf(value) · kindScale(kind)
//
// so a kind carries a meaningful floor (kindBase) even at cold start when IDF is
// uninformative, and rarity (IDF) sharpens it as the corpus grows. A name/gender
// is decisive (high base), an identifier strong, free context weak. A ubiquitous
// value (role mailbox, common name) decays toward its base as df → N.
const (
	kindBaseFunctional  = 1.0
	kindBaseSet         = 0.5
	kindBaseContextual  = 0.05
	kindScaleFunctional = 1.0
	kindScaleSet        = 0.7
	kindScaleContextual = 0.15
)

func kindBase(kind AttrKind) float64 {
	switch kind {
	case KindFunctional:
		return kindBaseFunctional
	case KindSet:
		return kindBaseSet
	default:
		return kindBaseContextual
	}
}

func kindScale(kind AttrKind) float64 {
	switch kind {
	case KindFunctional:
		return kindScaleFunctional
	case KindSet:
		return kindScaleSet
	default:
		return kindScaleContextual
	}
}

// idf is the smoothed inverse document frequency of a value: log of (corpus
// entity count) over (entities asserting the value), ≥ 0. It is ~0 both at cold
// start (where ω falls back to kindBase) and for ubiquitous values, and grows
// for rare ones.
func idf(docFreq, entities int) float64 {
	v := math.Log(float64(1+entities) / float64(1+docFreq))
	if v < 0 {
		return 0
	}

	return v
}

// omega is the identifying weight of a claim: how strongly a match on it should
// count toward (or against) same-identity. Source-trust and a temporal-validity
// factor are folded in as 1.0 until the importer populates them.
func omega(kind AttrKind, docFreq, entities int) float64 {
	return kindBase(kind) + idf(docFreq, entities)*kindScale(kind)
}
