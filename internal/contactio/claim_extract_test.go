// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"context"
	"strings"
	"testing"

	"blackcat.ca/gmeow/internal/embedding"
	"blackcat.ca/gmeow/internal/rdfbundle"
)

// extractClaim is a single-claim test shim over extractClaims (most predicates
// yield exactly one comparison claim; name predicates yield several tokens).
func extractClaim(s rdfbundle.Statement) (claimStatement, bool) {
	claims, ok := extractClaims(s)
	if !ok || len(claims) == 0 {
		return claimStatement{}, false
	}

	return claims[0], true
}

func TestClaimStatementsFromRootedTurtle(t *testing.T) {
	// Standard-vocabulary predicates ground on the ontology registry: foaf:name →
	// role-free name TOKENS (one claim per token), schema:email → concept "email"
	// (value NormEmail-canonicalized, mailto: stripped).
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

	var sawPatrick, sawAudley, sawEmail bool
	for _, s := range statements {
		switch s.Text {
		case "name-token: patrick":
			sawPatrick = true
			if !s.IsName {
				t.Fatalf("name token should be flagged IsName: %+v", s)
			}
		case "name-token: audley":
			sawAudley = true
		case "email: paudley@blackcat.ca":
			sawEmail = true
		}
	}
	if !sawPatrick || !sawAudley || !sawEmail {
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

// TestExtractClaimDropsUngroundedPredicate locks strict grounding: a predicate not
// in the ontology registry (an un-migrated importer's source-namespaced term) is
// NOT a comparison claim. Admitting these as contextual claims is what fused the
// blob (see extractClaim doc).
func TestExtractClaimDropsUngroundedPredicate(t *testing.T) {
	for _, pred := range []string{
		"https://gmeow.blackcat.ca/ns#applePropertyType",
		"https://gmeow.blackcat.ca/ns#appleEmailEntry",
		"https://gmeow.blackcat.ca/ns#outlookContactsLastName",
		"https://gmeow.blackcat.ca/ns#linkedInTime",
	} {
		if claim, ok := extractClaim(rdfStmt(pred, "Email")); ok {
			t.Fatalf(
				"ungrounded predicate %q must be dropped, got comparison claim %q",
				pred,
				claim.Text,
			)
		}
	}
	// A grounded predicate alongside them still resolves.
	if claim, ok := extractClaim(
		rdfStmt("https://schema.org/email", "real@person.example"),
	); !ok ||
		claim.Text != "email: real@person.example" {
		t.Fatalf("grounded predicate must still resolve, got ok=%v claim=%q", ok, claim.Text)
	}
}

// TestUngroundedScaffoldingDoesNotFuseEntities is the blob regression in miniature:
// two records that are clearly DIFFERENT people (distinct grounded emails+names) but
// share a large block of identical ungrounded scaffolding tokens must resolve to
// SEPARATE entities. Before strict grounding, the shared scaffolding accreted enough
// contextual IC mass to merge them — at corpus scale that fused ~2000 people into one
// entity (1 name, 1909 emails, 100k claims).
func TestUngroundedScaffoldingDoesNotFuseEntities(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver()
	const threshold = 0.5

	// A wide block of identical vendor scaffolding shared by every record of a format.
	scaffold := func(subject string) string {
		s := ""
		for _, pred := range []string{
			"applePropertyType", "appleEmailEntry", "applePhoneEntry", "appleAddressEntry",
			"applePersonFlags", "appleCreationTime", "outlookContactsCategories", "linkedInTime",
		} {
			s += "<" + subject + "> <https://gmeow.blackcat.ca/ns#" + pred + "> \"shared-token\" .\n"
		}

		return s
	}

	alice := `<urn:obs:a> <https://schema.org/name> "Alice Anderson" .
<urn:obs:a> <https://schema.org/email> <mailto:alice@anderson.example> .
` + scaffold("urn:obs:a")
	bob := `<urn:obs:b> <https://schema.org/name> "Bob Brown" .
<urn:obs:b> <https://schema.org/email> <mailto:bob@brown.example> .
` + scaffold("urn:obs:b")

	ra, err := resolver.Resolve(
		ctx,
		claimInputs(claimStatementsFromBody(alice)),
		threshold,
		threshold,
	)
	if err != nil {
		t.Fatalf("resolve alice: %v", err)
	}
	rb, err := resolver.Resolve(
		ctx,
		claimInputs(claimStatementsFromBody(bob)),
		threshold,
		threshold,
	)
	if err != nil {
		t.Fatalf("resolve bob: %v", err)
	}
	if !ra.IsNew || !rb.IsNew || ra.Entity == rb.Entity {
		t.Fatalf("shared scaffolding fused distinct people: alice=%+v bob=%+v", ra, rb)
	}
}

// TestSynthesizeFullNameFromParts: a record with given+family but no full name
// gains a functional schema:name claim derived from the parts (the conservative-
// ingest veto signal that separates different people; see synthesizeFullName).
// TestNamePartsDecomposeToTokens: name-bearing predicates (parts AND a full name)
// all decompose to role-free name TOKENS; the whole-name string is never itself a
// comparison claim, and a token shared by the parts and the full name dedups.
func TestNamePartsDecomposeToTokens(t *testing.T) {
	body := `<urn:x> <https://schema.org/givenName> "Reuven" .
<urn:x> <https://schema.org/familyName> "Cohen" .
<urn:x> <https://schema.org/name> "Reuven Q Cohen" .`

	got := map[string]bool{}
	for _, c := range claimStatementsFromBody(body) {
		got[c.Text] = true
		if strings.HasPrefix(c.Text, "name: ") {
			t.Fatalf("whole-name string must not be a comparison claim: %q", c.Text)
		}
	}
	for _, want := range []string{"name-token: reuven", "name-token: cohen", "name-token: q"} {
		if !got[want] {
			t.Fatalf("missing %q; got %v", want, got)
		}
	}
}

// TestSynthesizedFullNameSeparatesDifferentPeople: two different people sharing only
// a first name (Apple-style parts, no full name) must NOT fuse — the synthesized full
// name makes their differing names a functional contradiction (IAC veto). This is the
// contextual-blob guard at ingest; the safe over-split direction.
func TestSynthesizedFullNameSeparatesDifferentPeople(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver()
	const threshold = 0.5

	a := `<urn:obs:a> <https://schema.org/givenName> "Reuven" .
<urn:obs:a> <https://schema.org/familyName> "Cohen" .
<urn:obs:a> <https://schema.org/email> <mailto:reuven.cohen@a.example> .`
	b := `<urn:obs:b> <https://schema.org/givenName> "Reuven" .
<urn:obs:b> <https://schema.org/familyName> "Goldberg" .
<urn:obs:b> <https://schema.org/email> <mailto:reuven.goldberg@b.example> .`

	ra, err := resolver.Resolve(
		ctx,
		claimInputs(claimStatementsFromBody(a)),
		threshold,
		threshold,
	)
	if err != nil {
		t.Fatalf("resolve a: %v", err)
	}
	rb, err := resolver.Resolve(
		ctx,
		claimInputs(claimStatementsFromBody(b)),
		threshold,
		threshold,
	)
	if err != nil {
		t.Fatalf("resolve b: %v", err)
	}
	if !ra.IsNew || !rb.IsNew || ra.Entity == rb.Entity {
		t.Fatalf("shared first name fused distinct people: a=%+v b=%+v", ra, rb)
	}
}

// TestParseValidTimeFromTenureNode: a claim on a node carrying a reified OWL-Time
// interval (the index.ttl org:Membership pattern) inherits that VALID-time interval;
// an envelope-format claim (no temporal structure) stays unbounded.
func TestParseValidTimeFromTenureNode(t *testing.T) {
	body := `@prefix schema: <https://schema.org/> .
<urn:membership:1> schema:worksFor "Axion Internet" ;
    schema:startDate "1996-05-01" ;
    schema:endDate "1997-08-01" .
<urn:card:bob> schema:email <mailto:bob@bob.example> .`

	var worksFor, email *claimStatement
	for i, c := range claimStatementsFromBody(body) {
		switch {
		case strings.HasPrefix(c.Text, "works-for: "):
			worksFor = &claimStatementsFromBody(body)[i]
		case strings.HasPrefix(c.Text, "email: "):
			email = &claimStatementsFromBody(body)[i]
		}
	}
	if worksFor == nil || worksFor.ValidFrom != "1996-05-01T00:00:00Z" ||
		worksFor.ValidUntil != "1997-08-01T00:00:00Z" {
		t.Fatalf("tenure-node validity not parsed onto works-for claim: %+v", worksFor)
	}
	if email == nil || email.ValidFrom != "" || email.ValidUntil != "" {
		t.Fatalf("envelope-format email claim must stay unbounded: %+v", email)
	}
}

func rdfStmt(predicate, object string) rdfbundle.Statement {
	return rdfbundle.Statement{
		Predicate: rdfbundle.Term{Kind: "iri", Value: predicate},
		Object:    rdfbundle.Term{Kind: "literal", Value: object},
	}
}
