// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package embedding

import (
	"strings"
	"time"
)

// AttrKind is the resolution semantics of a claim's attribute, per the formal
// model in docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4.1. It determines
// how a claim contributes to idDiff:
//   - functional: single-valued-at-a-time (legal name, gender, birthdate); a
//     co-valid, unsuperseded mismatch is a contradiction (IAC).
//   - set: multi-valued identifiers (emails, phones, URLs, nicknames); scored by
//     identifying-mass overlap.
//   - contextual: free signal (notes, org, title); a soft, low-weight cosine web.
type AttrKind uint8

const (
	KindContextual AttrKind = iota
	KindFunctional
	KindSet
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

// functionalAttrs are single-valued-at-a-time identity attributes: a
// contemporaneous, unsuperseded disagreement on one is a real contradiction.
var functionalAttrs = map[string]bool{
	"name": true, "fn": true, "fullname": true, "formattedname": true,
	"gender": true, "sex": true,
	"bday": true, "birthday": true, "birthdate": true, "deathdate": true,
}

// setAttrs are multi-valued identifiers: a person legitimately holds several, so
// they are scored by overlap, never as a contradiction on non-overlap alone.
var setAttrs = map[string]bool{
	"email": true, "hasemail": true, "mbox": true,
	"telephone": true, "hastelephone": true, "tel": true, "phone": true,
	"url": true, "hasurl": true, "homepage": true, "weblog": true, "seealso": true,
	"nick": true, "nickname": true,
	"impp": true, "hasinstantmessage": true, "account": true,
	"sameas": true, "exactmatch": true,
}

// kindFor classifies an attribute local-name into its resolution kind. Unknown
// attributes are contextual (low-weight soft signal) — the safe default.
func kindFor(attr string) AttrKind {
	switch {
	case functionalAttrs[attr]:
		return KindFunctional
	case setAttrs[attr]:
		return KindSet
	default:
		return KindContextual
	}
}
