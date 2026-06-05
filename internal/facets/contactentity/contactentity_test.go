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
			Predicate:     "https://blackcatinformatics.ca/gmeow/contactAlias",
			Object:        "fixture handle",
			ObjectKind:    "literal",
		},
		{
			SourceDigest:  "sha256:source",
			StatementHash: "historical",
			Subject:       "https://example.test/#org",
			Predicate:     "https://blackcatinformatics.ca/gmeow/historicalEmail",
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

// TestLocationSubNodesFlattenOntoContact: address-component and coordinate
// literals live on the gmeow:addr / gmeow:place sub-nodes (linked from the contact
// via gmeow:hasContactPoint / gmeow:locatedAt), and timezone on the contact itself.
// The projection must attribute the sub-node literals back to the owning contact,
// while a sub-node no contact links must NOT leak.
func TestLocationSubNodesFlattenOntoContact(t *testing.T) {
	const contact = "https://example.test/#person"
	const addr = "urn:gmeow:addr:abc"
	const place = "urn:gmeow:place:xyz"
	statements := []Statement{
		{
			StatementHash: "type",
			Subject:       contact,
			Predicate:     rdfTypePredicate,
			Object:        schemaOrgPerson,
		},
		{
			StatementHash: "link-addr",
			Subject:       contact,
			Predicate:     gmeowPrefix + "hasContactPoint",
			Object:        addr,
			ObjectKind:    "iri",
		},
		{
			StatementHash: "link-place",
			Subject:       contact,
			Predicate:     gmeowPrefix + "locatedAt",
			Object:        place,
			ObjectKind:    "iri",
		},
		{
			StatementHash: "street",
			Subject:       addr,
			Predicate:     gmeowPrefix + "streetAddress",
			Object:        "1 Example Street",
			ObjectKind:    "literal",
		},
		{
			StatementHash: "city",
			Subject:       addr,
			Predicate:     gmeowPrefix + "addressLocality",
			Object:        "Example City",
			ObjectKind:    "literal",
		},
		{
			StatementHash: "lat",
			Subject:       place,
			Predicate:     gmeowPrefix + "latitude",
			Object:        "37.7",
			ObjectKind:    "literal",
		},
		{
			StatementHash: "tz",
			Subject:       contact,
			Predicate:     gmeowPrefix + "timezone",
			Object:        "America/Toronto",
			ObjectKind:    "literal",
		},
		// Orphan sub-node no contact links — must not surface.
		{
			StatementHash: "orphan",
			Subject:       "urn:gmeow:addr:orphan",
			Predicate:     gmeowPrefix + "streetAddress",
			Object:        "99 Nowhere",
			ObjectKind:    "literal",
		},
	}

	facts := FactsFromStatements(statements, nil)

	for _, want := range []struct {
		value string
		kind  string
	}{
		{"1 Example Street", FactKindAddress},
		{"Example City", FactKindAddress},
		{"37.7", FactKindCoordinates},
		{"America/Toronto", FactKindTimezone},
	} {
		fact := factByValue(facts, want.value)
		if fact.FactKind != want.kind || fact.ContactID != contact {
			t.Fatalf(
				"fact %q = %#v, want kind %s attributed to %s",
				want.value,
				fact,
				want.kind,
				contact,
			)
		}
	}

	if leaked := factByValue(facts, "99 Nowhere"); leaked.Value != "" {
		t.Fatalf("orphan sub-node leaked into projection: %#v", leaked)
	}
}

// TestPersonNameNodeFlattensOntoContactAndHonorsDisplayable: a gmeow:PersonName
// appellation's fullName/parts flatten onto the linking contact as name facts, a
// nickname projects as an alias, and a gmeow:displayable=false appellation (deadname)
// is suppressed entirely.
func TestPersonNameNodeFlattensOntoContactAndHonorsDisplayable(t *testing.T) {
	const contact = "https://example.test/#person"
	const chosen = "urn:gmeow:name:chosen"
	const dead = "urn:gmeow:name:dead"
	statements := []Statement{
		{
			StatementHash: "type",
			Subject:       contact,
			Predicate:     rdfTypePredicate,
			Object:        schemaOrgPerson,
		},
		{
			StatementHash: "l1",
			Subject:       contact,
			Predicate:     gmeowPrefix + "hasName",
			Object:        chosen,
			ObjectKind:    "iri",
		},
		{
			StatementHash: "full",
			Subject:       chosen,
			Predicate:     gmeowPrefix + "fullName",
			Object:        "Alex Rivera",
			ObjectKind:    "literal",
		},
		// Nickname is a TYPED gmeow:NamePart (two hops: contact -> appellation -> part).
		{
			StatementHash: "hp",
			Subject:       chosen,
			Predicate:     gmeowPrefix + "hasNamePart",
			Object:        "urn:gmeow:name:chosen#nick",
			ObjectKind:    "iri",
		},
		{
			StatementHash: "nt",
			Subject:       "urn:gmeow:name:chosen#nick",
			Predicate:     gmeowPrefix + "namePartType",
			Object:        gmeowPrefix + "namePartNickname",
			ObjectKind:    "iri",
		},
		{
			StatementHash: "pt",
			Subject:       "urn:gmeow:name:chosen#nick",
			Predicate:     gmeowPrefix + "partText",
			Object:        "Al",
			ObjectKind:    "literal",
		},
		// A suppressed deadname appellation — must not surface.
		{
			StatementHash: "l2",
			Subject:       contact,
			Predicate:     gmeowPrefix + "hasName",
			Object:        dead,
			ObjectKind:    "iri",
		},
		{
			StatementHash: "deadfull",
			Subject:       dead,
			Predicate:     gmeowPrefix + "fullName",
			Object:        "Deadname Rivera",
			ObjectKind:    "literal",
		},
		{
			StatementHash: "flag",
			Subject:       dead,
			Predicate:     gmeowPrefix + "displayable",
			Object:        "false",
			ObjectKind:    "literal",
		},
	}

	facts := FactsFromStatements(statements, nil)

	full := factByValue(facts, "Alex Rivera")
	if full.FactKind != FactKindName || full.ContactID != contact {
		t.Fatalf("fullName fact = %#v, want name attributed to contact", full)
	}
	if al := factByValue(facts, "Al"); al.FactKind != FactKindAlias {
		t.Fatalf("nickname should project as alias, got %#v", al)
	}
	if dn := factByValue(facts, "Deadname Rivera"); dn.Value != "" {
		t.Fatalf("displayable=false deadname leaked into projection: %#v", dn)
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

// TestFactsValidFromIsValidTimeOnly: the projection's ValidFrom comes ONLY from
// real VALID time (gmeow:validFrom), never from transaction/carrier/derived clocks.
// An envelope-format claim carrying only a recordedNoLaterThan (or the old
// observedAt) gets NO ValidFrom — the four-clock bug fix.
func TestFactsValidFromIsValidTimeOnly(t *testing.T) {
	const entity = "urn:gmeow:entity:01ENTITY"
	statement := Statement{
		SourceDigest:  "digest-1",
		StatementHash: "hash-1",
		Subject:       entity,
		Predicate:     "http://www.w3.org/2006/vcard/ns#hasEmail",
		Object:        "mailto:paudley@blackcat.ca",
		ObjectKind:    "iri",
	}
	annotate := func(pred, obj string) Annotation {
		return Annotation{
			SourceDigest:  "digest-1",
			StatementHash: "hash-1",
			Predicate:     pred,
			Object:        obj,
		}
	}

	// Envelope claim: only a derived recordedNoLaterThan (the carrier-derived bound)
	// → ValidFrom MUST stay empty (no fabrication).
	envelope := FactsForContacts([]Statement{statement}, []Annotation{
		annotate(
			"https://blackcatinformatics.ca/gmeow/recordedNoLaterThan",
			"2009-03-14T00:00:00Z",
		),
	}, map[string]bool{entity: true})
	if len(envelope) != 1 || envelope[0].ValidFrom != "" {
		t.Fatalf("envelope-format claim must have empty ValidFrom: %+v", envelope)
	}

	// Grounded claim: real gmeow:validFrom/validUntil → populated valid axis.
	grounded := FactsForContacts([]Statement{statement}, []Annotation{
		annotate("https://blackcatinformatics.ca/gmeow/validFrom", "1996-05-01T00:00:00Z"),
		annotate("https://blackcatinformatics.ca/gmeow/validUntil", "1997-08-01T00:00:00Z"),
	}, map[string]bool{entity: true})
	if len(grounded) != 1 || grounded[0].ValidFrom != "1996-05-01T00:00:00Z" ||
		grounded[0].ValidUntil != "1997-08-01T00:00:00Z" {
		t.Fatalf("grounded valid time not surfaced: %+v", grounded)
	}
}
