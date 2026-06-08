// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/ontology"
)

// AttrKind is the resolution semantics of a claim's attribute (functional / set
// / contextual), per docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4.1. It is
// the ontology's Kind — there is one authority for concept→kind
// (ontology.KindForConcept), so the engine never classifies attributes itself.
type (
	AttrKind = ontology.Kind
	AttrRole = ontology.Role
)

const (
	KindContextual = ontology.Contextual
	KindFunctional = ontology.Functional
	KindSet        = ontology.Set
	KindName       = ontology.Name
)

// Interval is a half-open validity window; a zero bound is unbounded on that
// side. It gates value-matches: a shared value only correlates two identities
// while they hold it contemporaneously (a transferred number / inherited role
// mailbox has disjoint intervals and so does not correlate).
type Interval struct {
	First time.Time
	Last  time.Time
}

// Overlaps reports whether two validity windows are contemporaneous (unbounded
// bounds extend to infinity). Two zero intervals (fully unbounded) always
// overlap — the graceful-degradation default until per-claim validity is
// populated from the importer.
func (i Interval) Overlaps(o Interval) bool {
	if !i.First.IsZero() && !o.Last.IsZero() && i.First.After(o.Last) {
		return false
	}

	if !o.First.IsZero() && !i.Last.IsZero() && o.First.After(i.Last) {
		return false
	}

	return true
}

// scoredClaim is the structured, in-memory form of a claim used by idDiff: the
// attribute + normalized value, its embedding, its resolution kind, and the
// optional temporal / provenance / supersedes metadata. Vec is sourced from the
// claim-vector cache (keyed by Hash) and is never persisted on the claim.
type scoredClaim struct {
	Attr       string
	Value      string
	Hash       string
	Kind       AttrKind
	Role       AttrRole
	Vec        Vector
	Valid      Interval
	SourceKey  string
	ULID       string
	Supersedes []string
}

// splitClaimText recovers (attribute, value) from the embedded claim text, which
// is rendered as "predicateLocalName: normalizedValue" (see
// contactio.claimText). The attribute drives kind classification and per-attr
// grouping in idDiff; the value is the identity-bearing payload.
func splitClaimText(text string) (string, string) {
	if attr, value, found := strings.Cut(text, ": "); found {
		return strings.ToLower(strings.TrimSpace(attr)), value
	}

	return strings.ToLower(strings.TrimSpace(text)), ""
}
