// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import "testing"

func TestParseRDFBundlePreservesUnknownAndRDFStarAnnotations(t *testing.T) {
	content := `@prefix bcid: <https://patrickaudley.com/lod#> .
@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix time: <http://www.w3.org/2006/time#> .

<https://patrickaudley.com/#paudley> a foaf:Person, schema:Person ;
    foaf:name "Patrick Colm Audley"@en ;
    bcid:historicalEmail <mailto:paudley@gt.ca> ;
    schema:knowsAbout <https://patrickaudley.com/#concept-linked-data> .

<< <https://patrickaudley.com/#paudley> bcid:historicalEmail <mailto:paudley@gt.ca> >>
    time:hasEnd "2004-06-30"^^<http://www.w3.org/2001/XMLSchema#date> .
`
	statements, annotations, err := parseRDFBundle(content)
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
		if statement.predicate.value == "https://schema.org/knowsAbout" {
			unknownFound = true
		}
		if statement.predicate.value == "https://patrickaudley.com/lod#historicalEmail" {
			emailHash = statement.hash
		}
	}
	if !unknownFound {
		t.Fatal("unknown contact predicate was not preserved in generic RDF statements")
	}
	if emailHash == "" {
		t.Fatal("historical email statement was not parsed")
	}
	if annotations[0].hash != emailHash {
		t.Fatalf(
			"annotation hash %q did not target email hash %q",
			annotations[0].hash,
			emailHash,
		)
	}
}

func TestParseLiteralTermPreservesEscapedQuotes(t *testing.T) {
	term := parseLiteralTerm(`"Patrick \"Pat\" Audley"@en`, nil)
	if term.value != `Patrick \"Pat\" Audley` {
		t.Fatalf("escaped literal was not preserved: %#v", term)
	}
	if term.language != "en" {
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
	if angle.datatype != "http://www.w3.org/2001/XMLSchema#date" ||
		prefixed.datatype != angle.datatype {
		t.Fatalf("datatype IRIs were not normalized: angle=%#v prefixed=%#v", angle, prefixed)
	}
}

func TestRDFStatementHashIsStable(t *testing.T) {
	statement := rdfStatement{
		subject:   rdfTerm{kind: "iri", value: "https://patrickaudley.com/#paudley"},
		predicate: rdfTerm{kind: "iri", value: "https://schema.org/email"},
		object:    rdfTerm{kind: "iri", value: "mailto:paudley@blackcat.ca"},
	}
	first := rdfStatementHash(statement)
	second := rdfStatementHash(statement)
	if first == "" || first != second {
		t.Fatalf("unstable statement hash first=%q second=%q", first, second)
	}
	statement.object.value = "mailto:paudley@example.test"
	if first == rdfStatementHash(statement) {
		t.Fatal("statement hash did not change when object changed")
	}
}
