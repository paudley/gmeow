// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"strings"

	"blackcat.ca/gmeow/internal/embedding"
	"blackcat.ca/gmeow/internal/ontology"
	"blackcat.ca/gmeow/internal/rdfbundle"
)

// claimStatement is the record-side unit of a claim: its identity-bearing text
// and subject-independent hash (mapped to embedding.ClaimInput for resolution),
// the Turtle Line persisted in a delta record, whether it is a name claim, and
// the original Subject it was asserted under (the delta builder needs the subject
// to preserve node sub-graphs — re-subjecting only the root agent to the entity).
type claimStatement struct {
	Subject string
	Text    string
	Hash    string
	Line    string
	IsName  bool
}

// claimStatementsFromBody turns a record's Turtle body (a raw rooted graph like
// index.ttl OR a generated line-oriented body) into the subject-INDEPENDENT
// claim statements ingest-time resolution consumes. The subject is dropped on
// purpose: identity lives in the predicate+object (the same email/name must
// collide across observations regardless of the local subject they were minted
// under). Duplicate claims within a record collapse to one.
func claimStatementsFromBody(body string) []claimStatement {
	statements, _, err := rdfbundle.Parse(body)
	if err != nil {
		return nil
	}

	return claimStatementsFromStatements(statements)
}

// claimStatementsFromStatements extracts the deduped comparison claims from
// already-parsed statements (the resolver input). Subject is retained but the
// dedup is by subject-independent hash (the same value under different node
// subjects is one comparison claim).
func claimStatementsFromStatements(statements []rdfbundle.Statement) []claimStatement {
	out := make([]claimStatement, 0, len(statements))
	seen := make(map[string]bool, len(statements))

	for _, statement := range statements {
		claim, ok := extractClaim(statement)
		if !ok {
			continue
		}
		if seen[claim.Hash] {
			continue
		}
		seen[claim.Hash] = true
		out = append(out, claim)
	}

	return out
}

// scaffoldingPredicates are the node-structure linking predicates that carry no
// comparison value (the value lives on the node they point to). They are dropped
// from the comparison claim set. rdf:type is deliberately NOT here: the entity's
// type triple (foaf:Person) must persist in the delta for the projection, and it
// is harmless in idDiff (every person shares it, contextual, low ω).
var scaffoldingPredicates = map[string]bool{
	schemaContactPointPred:                   true,
	schemaAboutPred:                          true,
	schemaAddressPred:                        true,
	ontology.FOAF + "accountName":            true,
	ontology.FOAF + "accountServiceHomepage": true,
}

// extractClaim turns one parsed statement into a subject-INDEPENDENT comparison
// claim, grounded in the canonical ontology. Predicates resolve to their
// canonical Concept + per-concept value normalization via ontology.ByIRI;
// node-structure scaffolding is dropped; an un-migrated legacy predicate (a
// source-namespaced importer term not yet on standards) falls back to its local
// name + generic normalization so it still resolves until that importer lands.
func extractClaim(statement rdfbundle.Statement) (claimStatement, bool) {
	predicate := statement.Predicate.Value
	if scaffoldingPredicates[predicate] {
		return claimStatement{}, false
	}

	var concept, value string
	isName := false

	if term, ok := ontology.ByIRI(predicate); ok {
		concept = term.Concept
		value = term.Normalize(statement.Object.Value)
		isName = concept == "name" || concept == "nickname"
	} else { // legacy fallback (un-migrated importer predicate)
		concept = predicateLocalName(predicate)
		value = normalizeFingerprintValue(statement.Object.Value)
		isName = isNameLocalName(concept)
	}

	if concept == "" || value == "" {
		return claimStatement{}, false
	}

	text := concept + ": " + value

	return claimStatement{
		Subject: statement.Subject.Value,
		Text:    text,
		Hash:    embedding.StatementHash(text),
		Line:    claimLine(statement),
		IsName:  isName,
	}, true
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

// isNameLocalName reports whether a (fallback) predicate local-name denotes a
// person/org name, so its object also seeds the entity's name-vector set.
func isNameLocalName(localName string) bool {
	switch strings.ToLower(localName) {
	case "name", "fn", "fullname", "formattedname", "nick", "nickname":
		return true
	default:
		return false
	}
}
