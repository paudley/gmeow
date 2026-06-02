// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package rdfbundle

import "testing"

func TestParsePreservesUnknownAndRDFStarAnnotations(t *testing.T) {
	content := `@prefix gmeow: <https://blackcatinformatics.ca/gmeow/> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix time: <http://www.w3.org/2006/time#> .

<https://example.test/#synthetic-contact> a foaf:Person, schema:Person ;
    foaf:name "Synthetic Contact"@en ;
    gmeow:historicalEmail <mailto:synthetic.legacy@example.test> ;
    schema:knowsAbout <https://example.test/#concept-linked-data> .

<< <https://example.test/#synthetic-contact> gmeow:historicalEmail <mailto:synthetic.legacy@example.test> >>
    time:hasEnd "2004-06-30"^^<http://www.w3.org/2001/XMLSchema#date> .
`
	statements, annotations, err := Parse(content)
	if err != nil {
		t.Fatal(err)
	}
	if len(statements) < 5 {
		t.Fatalf("expected parsed statements, got %#v", statements)
	}
	if len(annotations) != 1 {
		t.Fatalf("expected one RDF-star annotation, got %#v", annotations)
	}
	unknownFound := false
	emailHash := ""
	for _, statement := range statements {
		if statement.Predicate.Value == "https://schema.org/knowsAbout" {
			unknownFound = true
		}
		if statement.Predicate.Value == "https://blackcatinformatics.ca/gmeow/historicalEmail" {
			emailHash = statement.Hash
		}
	}
	if !unknownFound {
		t.Fatal("unknown contact predicate was not preserved in generic RDF statements")
	}
	if emailHash == "" {
		t.Fatal("historical email statement was not parsed")
	}
	if annotations[0].Hash != emailHash {
		t.Fatalf(
			"annotation hash %q did not target email hash %q",
			annotations[0].Hash,
			emailHash,
		)
	}
}

func TestParseLiteralTermPreservesEscapedQuotes(t *testing.T) {
	term := parseLiteralTerm(`"Synthetic \"Alias\" Contact"@en`, nil)
	if term.Value != `Synthetic \"Alias\" Contact` {
		t.Fatalf("escaped literal was not preserved: %#v", term)
	}
	if term.Language != "en" {
		t.Fatalf("language tag was not preserved: %#v", term)
	}
}

func TestParseLiteralTermNormalizesDatatypeIRI(t *testing.T) {
	prefixes := map[string]string{
		"xsd": "http://www.w3.org/2001/XMLSchema#",
	}
	angle := parseLiteralTerm(
		`"2004-06-30"^^<http://www.w3.org/2001/XMLSchema#date>`,
		prefixes,
	)
	prefixed := parseLiteralTerm(`"2004-06-30"^^xsd:date`, prefixes)
	if angle.Datatype != "http://www.w3.org/2001/XMLSchema#date" ||
		prefixed.Datatype != angle.Datatype {
		t.Fatalf("datatype IRIs were not normalized: angle=%#v prefixed=%#v", angle, prefixed)
	}
}

func TestStatementHashIsStable(t *testing.T) {
	statement := Statement{
		Subject:   Term{Kind: "iri", Value: "https://example.test/#synthetic-contact"},
		Predicate: Term{Kind: "iri", Value: "https://schema.org/email"},
		Object:    Term{Kind: "iri", Value: "mailto:synthetic.primary@example.test"},
	}
	first := StatementHash(statement)
	second := StatementHash(statement)
	if first == "" || first != second {
		t.Fatalf("unstable statement hash first=%q second=%q", first, second)
	}
	statement.Object.Value = "mailto:synthetic.other@example.test"
	if first == StatementHash(statement) {
		t.Fatal("statement hash did not change when object changed")
	}
}
