// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"context"
	"testing"

	"blackcat.ca/gmeow/internal/embedding"
)

func TestClaimStatementsFromRootedTurtle(t *testing.T) {
	body := `@prefix gmeow: <https://blackcatinformatics.ca/gmeow/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .

<https://example.test/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    gmeow:hasEmail <mailto:paudley@blackcat.ca> .
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
		case "hasEmail: mailto:paudley@blackcat.ca":
			sawEmail = true
		}
	}
	if !sawName || !sawEmail {
		t.Fatalf("missing expected claims; got %+v", statements)
	}
}

// TestFullChainTurtleToResolution wires the shared parser through claim
// extraction into the resolver: a rooted graph mints one entity; a vCard-style
// body for the same person with a new email matches it and yields a 1-claim delta.
func TestFullChainTurtleToResolution(t *testing.T) {
	ctx := context.Background()
	resolver := newTestResolver()
	const threshold = 0.5

	indexTTL := `@prefix gmeow: <https://blackcatinformatics.ca/gmeow/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
<https://example.test/#paudley> a foaf:Person ;
    foaf:name "Patrick Audley" ;
    gmeow:hasEmail <mailto:paudley@blackcat.ca> ;
    gmeow:affiliation "Blackcat Informatics" .
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

	// A generated line-oriented vCard body: same person, one new email.
	vcardBody := `<urn:gmeow:observation:abc> <http://xmlns.com/foaf/0.1/name> "Patrick Audley" .
<urn:gmeow:observation:abc> <https://blackcatinformatics.ca/gmeow/hasEmail> <mailto:paudley@blackcat.ca> .
<urn:gmeow:observation:abc> <https://blackcatinformatics.ca/gmeow/hasEmail> <mailto:pat@new.example> .
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
	wantHash := embedding.StatementHash("hasEmail: mailto:pat@new.example")
	if len(second.NewClaimHashes) != 1 || second.NewClaimHashes[0] != wantHash {
		t.Fatalf("vcard delta=%+v, want exactly the new email", second.NewClaimHashes)
	}
}
