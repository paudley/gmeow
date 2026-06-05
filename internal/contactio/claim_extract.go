// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
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
	// ValidFrom/ValidUntil are the claim's VALID-time (tenure) bounds, RFC3339,
	// inherited from its bearing node (empty = unbounded). The only clock fed to the
	// co-validity gate (see temporal.go / the four-clock model).
	ValidFrom  string
	ValidUntil string
}

// claimStatementsFromBody turns a record's Turtle body (a raw rooted graph like
// index.ttl OR a generated line-oriented body) into the subject-INDEPENDENT
// claim statements ingest-time resolution consumes. The subject is dropped on
// purpose: identity lives in the predicate+object (the same email/name must
// collide across observations regardless of the local subject they were minted
// under). Duplicate claims within a record collapse to one.
func claimStatementsFromBody(body string) []claimStatement {
	statements, annotations, err := rdfbundle.Parse(body)
	if err != nil {
		return nil
	}

	return claimStatementsFromStatements(statements, annotations)
}

// claimStatementsFromStatements extracts the deduped comparison claims from
// already-parsed statements (the resolver input). Subject is retained but the
// dedup is by subject-independent hash (the same value under different node
// subjects is one comparison claim).
func claimStatementsFromStatements(
	statements []rdfbundle.Statement,
	annotations []rdfbundle.AnnotationRecord,
) []claimStatement {
	out := make([]claimStatement, 0, len(statements))
	seen := make(map[string]bool, len(statements))
	validity := bearingNodeValidity(statements, annotations)

	for _, statement := range statements {
		claims, ok := extractClaims(statement)
		if !ok {
			continue
		}
		for _, claim := range claims {
			if seen[claim.Hash] {
				continue
			}
			if v, ok := validity[claim.Subject]; ok {
				claim.ValidFrom, claim.ValidUntil = v.From, v.Until
			}
			seen[claim.Hash] = true
			out = append(out, claim)
		}
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

// extractClaims turns one parsed statement into the subject-INDEPENDENT comparison
// claim(s) it grounds, STRICTLY via the canonical ontology. A predicate resolves to
// its canonical Concept + per-concept value normalization via ontology.ByIRI;
// node-structure scaffolding is dropped. A predicate NOT in the registry is NOT a
// comparison claim — it is source provenance (kept in FILESTORE), never fed to
// idDiff/embedding/blocking.
//
// Most predicates yield exactly one claim. A NAME-bearing predicate (fullName, the
// part shortcuts, the schema/foaf/vcard aliases) is DECOMPOSED into role-free name
// TOKENS — one "name-token: <tok>" claim per normalized token, honorific and
// generational tokens stripped (ontology.NameTokens) — so the engine compares names
// by idf-subsumption (no privileged surname; see nameScore). The whole-name string
// is never itself a comparison claim.
//
// Strict grounding is load-bearing, not cosmetic: the prior legacy fallback admitted
// un-migrated source-namespaced predicates (apple/outlook/linkedin scaffolding such
// as appleEmailEntry, applePropertyType, outlookContactsLastName) as CONTEXTUAL
// claims. Because every record of a format shares those structural tokens, any two
// such records accreted enough shared-contextual IC mass to clear the merge gate —
// fusing thousands of unrelated people into one blob entity (observed: one entity,
// 1 name, 1909 distinct emails, 100k claims). Grounding strictly means an unmapped
// predicate can never become identity signal; the cost is that an un-migrated format
// under-merges (recoverable) instead of catastrophically over-merging (not).
func extractClaims(statement rdfbundle.Statement) ([]claimStatement, bool) {
	predicate := statement.Predicate.Value
	if scaffoldingPredicates[predicate] {
		return nil, false
	}

	term, ok := ontology.ByIRI(predicate)
	if !ok {
		return nil, false // ungrounded → provenance only, not a comparison claim
	}

	concept := term.Concept
	value := term.Normalize(statement.Object.Value)

	// Re-type the user's IM-over-email hack: "<jid>@<service>.i.blackcat.ca" is an
	// IM account, not an email — route it to the account concept so it leaves the
	// email identifier index (see ontology.IMAccountFromPseudoEmail).
	if concept == "email" {
		if account, ok := ontology.IMAccountFromPseudoEmail(value); ok {
			concept, value = "account", account
		}
	}

	if concept == "" || value == "" {
		return nil, false
	}

	subject := statement.Subject.Value
	line := claimLine(statement)

	if ontology.IsNameConcept(concept) {
		return nameTokenClaims(subject, line, value), true
	}

	text := concept + ": " + value

	return []claimStatement{{
		Subject: subject,
		Text:    text,
		Hash:    embedding.StatementHash(text),
		Line:    line,
		IsName:  false,
	}}, true
}

// nameTokenClaims decomposes a grounded name value into its role-free comparison
// tokens (one claim each). All tokens of a statement share its source Line; the
// delta builder keeps the statement when ANY of its tokens is new.
func nameTokenClaims(subject, line, value string) []claimStatement {
	tokens := ontology.NameTokens(value)
	out := make([]claimStatement, 0, len(tokens))
	for _, tok := range tokens {
		text := ontology.NameTokenConcept + ": " + tok
		out = append(out, claimStatement{
			Subject: subject,
			Text:    text,
			Hash:    embedding.StatementHash(text),
			Line:    line,
			IsName:  true,
		})
	}

	return out
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
