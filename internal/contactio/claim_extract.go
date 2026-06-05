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
		claim, ok := extractClaim(statement)
		if !ok {
			continue
		}
		if seen[claim.Hash] {
			continue
		}
		if v, ok := validity[claim.Subject]; ok {
			claim.ValidFrom, claim.ValidUntil = v.From, v.Until
		}
		seen[claim.Hash] = true
		out = append(out, claim)
	}

	return synthesizeFullName(out, seen)
}

// synthesizeFullName derives a functional schema:name claim from a record's
// given-name + family-name when it carries the parts but no full name (Apple,
// Outlook, LinkedIn export only parts). This is load-bearing for CONSERVATIVE
// ingest: the full name is the IDENTITY-bearing functional claim whose MISMATCH is
// the IAC veto that SEPARATES different people. Without it, parts-only records share
// only soft contextual evidence, and a large entity's broad contextual vocabulary
// (given+family+org+address parts) accretes unrelated people into one entity (the
// contextual blob — over-merge, the dangerous direction). With it, distinct full
// names veto, so ingest over-SPLITS instead (the safe, REPAIR-able direction). The
// residual over-split (same person, name-form variant) is consolidated by REPAIR's
// global signed partition, not by relaxing this veto.
func synthesizeFullName(
	claims []claimStatement,
	seen map[string]bool,
) []claimStatement {
	var given, family, subject string

	for _, c := range claims {
		switch {
		case strings.HasPrefix(c.Text, "name: "):
			return claims // a full name is already present
		case given == "" && strings.HasPrefix(c.Text, "given-name: "):
			given = strings.TrimPrefix(c.Text, "given-name: ")
			subject = c.Subject
		case family == "" && strings.HasPrefix(c.Text, "family-name: "):
			family = strings.TrimPrefix(c.Text, "family-name: ")
			if subject == "" {
				subject = c.Subject
			}
		}
	}

	if given == "" || family == "" {
		return claims
	}

	value := ontology.NormText(given + " " + family)
	text := "name: " + value
	if value == "" || seen[embedding.StatementHash(text)] {
		return claims
	}

	return append(claims, claimStatement{
		Subject: subject,
		Text:    text,
		Hash:    embedding.StatementHash(text),
		Line:    iri(ontology.Schema+"name") + " " + literal(value),
		IsName:  true,
	})
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
// claim, STRICTLY grounded in the canonical ontology. A predicate resolves to its
// canonical Concept + per-concept value normalization via ontology.ByIRI;
// node-structure scaffolding is dropped. A predicate NOT in the registry is NOT a
// comparison claim — it is source provenance (kept in FILESTORE), never fed to
// idDiff/embedding/blocking.
//
// This strictness is load-bearing, not cosmetic: the prior legacy fallback admitted
// un-migrated source-namespaced predicates (apple/outlook/linkedin scaffolding such
// as appleEmailEntry, applePropertyType, outlookContactsLastName) as CONTEXTUAL
// claims. Because every record of a format shares those structural tokens, any two
// such records accreted enough shared-contextual IC mass to clear the merge gate —
// fusing thousands of unrelated people into one blob entity (observed: one entity,
// 1 name, 1909 distinct emails, 100k claims). Grounding strictly means an unmapped
// predicate can never become identity signal; the cost is that an un-migrated format
// under-merges (recoverable) instead of catastrophically over-merging (not).
func extractClaim(statement rdfbundle.Statement) (claimStatement, bool) {
	predicate := statement.Predicate.Value
	if scaffoldingPredicates[predicate] {
		return claimStatement{}, false
	}

	term, ok := ontology.ByIRI(predicate)
	if !ok {
		return claimStatement{}, false // ungrounded → provenance only, not a comparison claim
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
		return claimStatement{}, false
	}

	text := concept + ": " + value

	return claimStatement{
		Subject: statement.Subject.Value,
		Text:    text,
		Hash:    embedding.StatementHash(text),
		Line:    claimLine(statement),
		IsName:  concept == "name" || concept == "nickname",
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
