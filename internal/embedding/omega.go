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
	// Name tokens sit between contextual and set: a low floor (a single shared given
	// name barely corroborates) but FULL idf sharpening (uncapped — a rare token like
	// "Audley" IS discriminating; there is no privileged "surname" slot, only rarity).
	// Calibrated so a lone rare token stays just below the merge gate while a full
	// name (two+ shared tokens) clears it; the divergence gate, not a cap, blocks the
	// name-fragment blob. See nameScore / CONTACT_IDENTITY_RESOLUTION.md §4.2.
	kindBaseName  = 0.15
	kindScaleName = 0.3
	// contextualIDFCap bounds the IDF sharpening for CONTEXTUAL values. Per §4.1,
	// contextual evidence is "soft, low-ω": a rare first/last name part, a small
	// company, an unusual note is NOT a discriminating identifier the way a rare
	// email is — it is shared by everyone who has that name/employer. Without this
	// cap, idf inflates a rare name part to ω≈0.9 and a single part-match clears
	// the 0.75 merge gate, accreting people who merely share a first name into one
	// entity (the name-fragment blob). Capped, max contextual ω ≈ 0.2 — corroborates
	// but never identifies. Functional (full name) and Set (identifiers) keep full
	// IDF: an unusual full name and a rare email ARE discriminating.
	contextualIDFCap = 1.0
)

func kindBase(kind AttrKind) float64 {
	switch kind {
	case KindFunctional:
		return kindBaseFunctional
	case KindSet:
		return kindBaseSet
	case KindName:
		return kindBaseName
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
	case KindName:
		return kindScaleName
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
	d := idf(docFreq, entities)
	if kind == KindContextual && d > contextualIDFCap {
		d = contextualIDFCap // contextual evidence stays soft/low-ω (§4.1)
	}

	return kindBase(kind) + d*kindScale(kind)
}
