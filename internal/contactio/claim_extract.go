// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"strings"

	"blackcat.ca/gmeow/internal/embedding"
	"blackcat.ca/gmeow/internal/rdfbundle"
)

// claimStatement is the record-side unit of a claim: its identity-bearing text
// and subject-independent hash (mapped to embedding.ClaimInput for resolution),
// the Turtle Line persisted in a delta record, and whether it is a name claim.
type claimStatement struct {
	Text   string
	Hash   string
	Line   string
	IsName bool
}

// claimStatementsFromBody turns a record's Turtle body (a raw rooted graph like
// index.ttl OR a generated line-oriented body) into the subject-INDEPENDENT
// claim statements ingest-time resolution consumes. The subject is dropped on
// purpose: identity lives in the predicate+object (the same email/name must
// collide across observations regardless of the local subject they were minted
// under). Duplicate claims within a record collapse to one.
func claimStatementsFromBody(body string) []claimStatement {
	statements, _, err := rdfbundle.Parse(body)
	if err != nil || len(statements) == 0 {
		return nil
	}

	out := make([]claimStatement, 0, len(statements))
	seen := make(map[string]bool, len(statements))
	for _, statement := range statements {
		text := claimText(statement)
		if text == "" {
			continue
		}
		hash := embedding.StatementHash(text)
		if seen[hash] {
			continue
		}
		seen[hash] = true
		out = append(out, claimStatement{
			Text:   text,
			Hash:   hash,
			Line:   claimLine(statement),
			IsName: isNamePredicate(statement.Predicate.Value),
		})
	}

	return out
}

// claimText is the embedded/diffed semantic form of a claim: the predicate's
// local name plus the normalized object value (lower-cased, whitespace-collapsed
// so case/spacing variants of the same email/name share a vector and a hash).
func claimText(statement rdfbundle.Statement) string {
	predicate := predicateLocalName(statement.Predicate.Value)
	object := normalizeClaimObject(predicate, statement.Object.Value)
	if predicate == "" || object == "" {
		return ""
	}

	return predicate + ": " + object
}

// normalizeClaimObject canonicalizes a claim's object value by TYPE so that
// formatting variants of the same value collapse to ONE cache key (and one
// embed, and one diffed claim). Without this, "+1 (555) 123-4567" and
// "5551234567" are distinct — a phone seen in three formats costs three embeds
// and never converges. The persisted record keeps the original value; only the
// embedded/keyed text is canonicalized.
func normalizeClaimObject(predicate, value string) string {
	switch strings.ToLower(predicate) {
	case "hastelephone", "telephone", "tel", "phone":
		return normalizePhoneValue(value)
	case "hasurl", "url", "homepage", "weblog", "seealso":
		return normalizeURLValue(value)
	default:
		return normalizeFingerprintValue(value)
	}
}

// normalizePhoneValue reduces a phone number to its dialable digits, dropping a
// leading North-American "1" so "+1 555…" and "555…" collapse. Non-numeric junk
// (extensions, labels) falls back to the generic normalizer.
func normalizePhoneValue(value string) string {
	var digits strings.Builder
	for _, r := range value {
		if r >= '0' && r <= '9' {
			digits.WriteRune(r)
		}
	}
	number := digits.String()
	if len(number) == 11 && number[0] == '1' {
		number = number[1:]
	}
	if number == "" {
		return normalizeFingerprintValue(value)
	}

	return number
}

// normalizeURLValue lower-cases and trims a trailing slash so "http://x.com" and
// "http://x.com/" collapse.
func normalizeURLValue(value string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(value)), "/")
}

// claimLine is the predicate+object Turtle fragment persisted in a delta record;
// the entity subject is prepended at write time (records are re-subjected to the
// resolved entity ULID).
func claimLine(statement rdfbundle.Statement) string {
	return iri(statement.Predicate.Value) + " " + renderObjectTerm(statement.Object)
}

func renderObjectTerm(term rdfbundle.Term) string {
	if term.Kind == "iri" {
		return iri(term.Value)
	}
	rendered := literal(term.Value)
	if term.Datatype != "" {
		return rendered + "^^" + iri(term.Datatype)
	}
	if term.Language != "" {
		return rendered + "@" + term.Language
	}

	return rendered
}

// predicateLocalName returns the fragment/last-path segment of a predicate IRI,
// e.g. ".../gmeow/hasEmail" -> "hasEmail", rdf:type -> "type".
func predicateLocalName(iriValue string) string {
	value := iriValue
	if idx := strings.LastIndexAny(value, "#/"); idx >= 0 && idx+1 < len(value) {
		value = value[idx+1:]
	}

	return strings.TrimSpace(value)
}

// isNamePredicate reports whether a predicate denotes a person/org name, so its
// object also seeds the entity's name-vector set (renames accumulate vectors).
func isNamePredicate(iriValue string) bool {
	switch strings.ToLower(predicateLocalName(iriValue)) {
	case "name", "fn", "fullname", "formattedname", "nick", "nickname":
		return true
	default:
		return false
	}
}
