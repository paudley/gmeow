// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"context"
	"testing"

	"blackcat.ca/gmeow/internal/embedding"
	"blackcat.ca/gmeow/internal/rdfbundle"
)

func TestClaimStatementsFromRootedTurtle(t *testing.T) {
	// Standard-vocabulary predicates ground on the ontology registry: foaf:name →
	// concept "name", schema:email → concept "email" (value NormEmail-canonicalized,
	// mailto: stripped).
	body := `@prefix schema: <https://schema.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<https://example.test/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    schema:email <mailto:paudley@blackcat.ca> .
`
	statements := claimStatementsFromBody(body)
	if len(statements) == 0 {
		t.Fatal("expected claim statements from rooted Turtle")
	}

	var sawName, sawEmail bool
	for _, s := range statements {
		switch s.Text {
		case "name: patrick audley":
			sawName = true
			if !s.IsName {
				t.Fatalf("name claim should be flagged IsName: %+v", s)
			}
		case "email: paudley@blackcat.ca":
			sawEmail = true
		}
	}
	if !sawName || !sawEmail {
		t.Fatalf("missing expected canonical claims; got %+v", statements)
	}
}

// TestFullChainTurtleToResolution wires the shared parser through registry-
// grounded extraction into the resolver: a rooted graph mints one entity; a
// line-oriented body for the same person with a new email matches it and yields
// a 1-claim delta. Cross-vocabulary by design: index.ttl uses foaf:name, the
// vCard-style body uses schema:email — both canonicalize to the same concepts.
func TestFullChainTurtleToResolution(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver()
	const threshold = 0.5

	indexTTL := `@prefix schema: <https://schema.org/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
<https://example.test/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    schema:email <mailto:paudley@blackcat.ca> ;
    schema:description "Blackcat Informatics" .
`
	first, err := resolver.Resolve(
		ctx,
		claimInputs(claimStatementsFromBody(indexTTL)),
		threshold,
		threshold,
	)
	if err != nil {
		t.Fatalf("resolve index.ttl: %v", err)
	}
	if !first.IsNew {
		t.Fatalf("index.ttl should mint a new entity, got %+v", first)
	}

	// A generated line-oriented body: same person, one new email.
	vcardBody := `<urn:gmeow:observation:abc> <https://schema.org/name> "Patrick Audley" .
<urn:gmeow:observation:abc> <https://schema.org/email> <mailto:paudley@blackcat.ca> .
<urn:gmeow:observation:abc> <https://schema.org/email> <mailto:pat@new.example> .
`
	second, err := resolver.Resolve(
		ctx,
		claimInputs(claimStatementsFromBody(vcardBody)),
		threshold,
		threshold,
	)
	if err != nil {
		t.Fatalf("resolve vcard: %v", err)
	}
	if second.Entity != first.Entity {
		t.Fatalf(
			"vcard resolved to %s, want same entity %s (sim=%v)",
			second.Entity,
			first.Entity,
			second.Similarity,
		)
	}
	wantHash := embedding.StatementHash("email: pat@new.example")
	if len(second.NewClaimHashes) != 1 || second.NewClaimHashes[0] != wantHash {
		t.Fatalf("vcard delta=%+v, want exactly the new email", second.NewClaimHashes)
	}
}

func TestExtractClaimCanonicalizesFormattingVariants(t *testing.T) {
	// Phone variants collapse via the ontology phone normalizer (schema:telephone).
	a, _ := extractClaim(rdfStmt("https://schema.org/telephone", "+1 (555) 123-4567"))
	b, _ := extractClaim(rdfStmt("https://schema.org/telephone", "555-123-4567"))
	c, _ := extractClaim(rdfStmt("https://schema.org/telephone", "5551234567"))
	if a.Text != b.Text || b.Text != c.Text {
		t.Fatalf("phone variants did not collapse: %q / %q / %q", a.Text, b.Text, c.Text)
	}
	if a.Text != "phone: 5551234567" {
		t.Fatalf("unexpected canonical phone text: %q", a.Text)
	}

	// Cross-vocabulary email convergence: foaf:mbox and schema:email → same claim.
	m, _ := extractClaim(
		rdfStmt("http://xmlns.com/foaf/0.1/mbox", "mailto:Pat@Example.com"),
	)
	e, _ := extractClaim(rdfStmt("https://schema.org/email", "PAT@example.com"))
	if m.Text != e.Text || m.Text != "email: pat@example.com" {
		t.Fatalf("email variants did not converge: %q / %q", m.Text, e.Text)
	}

	// Structural scaffolding is dropped (not a comparison claim).
	if _, ok := extractClaim(
		rdfStmt("https://schema.org/contactPoint", "mailto:x@y.com"),
	); ok {
		t.Fatalf("schema:contactPoint scaffolding must be dropped")
	}
}

func rdfStmt(predicate, object string) rdfbundle.Statement {
	return rdfbundle.Statement{
		Predicate: rdfbundle.Term{Kind: "iri", Value: predicate},
		Object:    rdfbundle.Term{Kind: "literal", Value: object},
	}
}
