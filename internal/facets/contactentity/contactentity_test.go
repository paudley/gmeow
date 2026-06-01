// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactentity

import (
	"reflect"
	"testing"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestMetadataBuildsStableFacetData(t *testing.T) {
	facet := Facet(MetadataInput{
		RootSubject:   " https://example.test/#org ",
		TargetSubject: " https://example.test/#target ",
		Format:        " text/turtle ",
		SourceKind:    " rdf ",
		ClaimKind:     " correction ",
		IdentityHints: []string{
			"MAILTO:Admin@Example.Test",
			"admin@example.test",
			"",
		},
	})

	if facet.Kind != contracts.ContactEntityFacetKind {
		t.Fatalf("facet kind = %q, want %q", facet.Kind, contracts.ContactEntityFacetKind)
	}
	if RootSubject(facet.Metadata) != "https://example.test/#org" {
		t.Fatalf("root subject was not normalized: %#v", facet.Metadata)
	}
	if TargetSubject(facet.Metadata) != "https://example.test/#target" {
		t.Fatalf("target subject was not normalized: %#v", facet.Metadata)
	}
	if Format(facet.Metadata) != "text/turtle" {
		t.Fatalf("format was not normalized: %#v", facet.Metadata)
	}

	hints, ok := facet.Metadata["identity_hints"].([]string)
	if !ok || !reflect.DeepEqual(hints, []string{"admin@example.test"}) {
		t.Fatalf("identity hints were not normalized: %#v", facet.Metadata)
	}

	if _, ok := facet.Metadata["last_seen_at"]; ok {
		t.Fatalf("mutable freshness leaked into contact metadata: %#v", facet.Metadata)
	}
}

func TestContactEntityTypeRecognizesNonPersonSubjects(t *testing.T) {
	statements := []Statement{
		{
			Subject:   "https://example.test/#org",
			Predicate: rdfTypePredicate,
			Object:    "http://schema.org/Organization",
		},
		{
			Subject:   "https://example.test/#person",
			Predicate: rdfTypePredicate,
			Object:    "http://xmlns.com/foaf/0.1/Person",
		},
	}

	contacts := ContactSubjects(statements)
	if !contacts["https://example.test/#org"] {
		t.Fatalf("organization subject was not recognized: %#v", contacts)
	}
	if !contacts["https://example.test/#person"] {
		t.Fatalf("person subject was not recognized: %#v", contacts)
	}
}

func TestFactsFromStatementsClassifiesAndNormalizesContactFacts(t *testing.T) {
	statements := []Statement{
		{
			SourceDigest:  "sha256:source",
			StatementHash: "type",
			Subject:       "https://example.test/#org",
			Predicate:     rdfTypePredicate,
			Object:        "http://schema.org/Organization",
		},
		{
			SourceDigest:  "sha256:source",
			StatementHash: "email",
			Subject:       "https://example.test/#org",
			Predicate:     "http://schema.org/email",
			Object:        "mailto:Admin@Example.Test",
			ObjectKind:    "iri",
		},
		{
			SourceDigest:  "sha256:source",
			StatementHash: "contact-alias",
			Subject:       "https://example.test/#org",
			Predicate:     "https://patrickaudley.com/lod#contactAlias",
			Object:        "fixture handle",
			ObjectKind:    "literal",
		},
		{
			SourceDigest:  "sha256:source",
			StatementHash: "historical",
			Subject:       "https://example.test/#org",
			Predicate:     "https://patrickaudley.com/lod#historicalEmail",
			Object:        "old@example.test",
			ObjectKind:    "literal",
		},
		{
			SourceDigest:  "sha256:source",
			StatementHash: "unknown",
			Subject:       "https://example.test/#org",
			Predicate:     "https://schema.org/knowsAbout",
			Object:        "linked data",
			ObjectKind:    "literal",
		},
	}
	annotations := []Annotation{{
		SourceDigest:  "sha256:source",
		StatementHash: "historical",
		Predicate:     "http://www.w3.org/2006/time#hasEnd",
		Object:        "2004-06-30",
	}}

	facts := FactsFromStatements(statements, annotations)
	if len(facts) != 3 {
		t.Fatalf("facts = %#v, want three classified contact facts", facts)
	}

	current := factByValue(facts, "admin@example.test")
	if current.FactKind != FactKindEmail || current.Historical {
		t.Fatalf("current email fact was not normalized: %#v", current)
	}

	historical := factByValue(facts, "old@example.test")
	if historical.FactKind != FactKindEmail ||
		!historical.Historical ||
		historical.ValidUntil != "2004-06-30" {
		t.Fatalf("historical email fact was not annotated: %#v", historical)
	}

	alias := factByValue(facts, "fixture-handle")
	if alias.FactKind != FactKindContactAlias {
		t.Fatalf("contact alias fact was not normalized: %#v", alias)
	}
}

func TestNormalizeIdentityHandlesMailtoAndDisplayNames(t *testing.T) {
	got := NormalizeIdentity(`Pat <MAILTO:Pat@Example.Test>`)
	if got != "pat@example.test" {
		t.Fatalf("NormalizeIdentity = %q, want pat@example.test", got)
	}

	got = NormalizeIdentity(`mailto.support@example.test`)
	if got != "mailto.support@example.test" {
		t.Fatalf("NormalizeIdentity = %q, want mailto.support@example.test", got)
	}
}

func TestNormalizeAliasBuildsSlugHandle(t *testing.T) {
	got := NormalizeAlias(" Fixture_Handle.One ")
	if got != "fixture-handle-one" {
		t.Fatalf("NormalizeAlias = %q, want fixture-handle-one", got)
	}

	if got := NormalizeAlias("..."); got != "" {
		t.Fatalf("punctuation-only alias normalized to %q, want empty", got)
	}
}

func factByValue(facts []Fact, value string) Fact {
	for _, fact := range facts {
		if fact.Value == value {
			return fact
		}
	}

	return Fact{}
}
