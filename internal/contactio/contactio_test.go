// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
)

func TestVCardImportProducesRDFContactBundle(t *testing.T) {
	object, result, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"alice.vcf",
		[]byte(
			"BEGIN:VCARD\nVERSION:4.0\nFN:Alice Example\nEMAIL:Alice@Example.Test\nORG:Example Org\nNOTE:synthetic only\nEND:VCARD\n",
		),
		time.Date(2026, 6, 1, 1, 2, 3, 0, time.UTC),
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, snippet := range []string{
		"<mailto:alice@example.test>",
		"<http://www.w3.org/2006/vcard/ns#hasEmail>",
		"\"Example Org\"",
		"\"synthetic only\"",
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("imported RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	if object.MediaType != MediaTypeTurtle ||
		object.SourceKind != contactImportSourceKind ||
		len(object.Facets) != 2 ||
		len(result.Contacts) != 1 ||
		result.Contacts[0] != "mailto:alice@example.test" {
		t.Fatalf("unexpected import object=%#v result=%#v", object, result)
	}
}

func TestNativeImportAndExportPreservesTemporalFacts(t *testing.T) {
	bundle := NativeBundle{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Contacts: []NativeContact{{
			ContactID:    "https://example.test/#alice",
			DisplayName:  "Alice Example",
			PrimaryEmail: "alice@example.test",
			Facts: []contracts.ContactFact{{
				ContactID:  "https://example.test/#alice",
				FactKind:   contactentity.FactKindEmail,
				Value:      "old@example.test",
				ValidUntil: "2020-01-01",
				Historical: true,
			}},
		}},
	}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}

	object, _, err := BuildImportObject(
		FormatNative,
		"contacts",
		"alice.json",
		encoded,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, snippet := range []string{
		"<https://example.test/#alice>",
		"<https://patrickaudley.com/lod#historicalEmail>",
		"<http://www.w3.org/2006/time#hasEnd>",
		"\"2020-01-01\"^^<http://www.w3.org/2001/XMLSchema#date>",
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("native RDF missing %q:\n%s", snippet, object.Content)
		}
	}

	exported, err := Export(FormatNative, []contracts.ContactAggregate{{
		ContactID:        "https://example.test/#alice",
		DisplayName:      "Alice Example",
		PrimaryEmail:     "alice@example.test",
		MessageCount:     99,
		ParticipantCount: 123,
		Facts:            bundle.Contacts[0].Facts,
	}})
	if err != nil {
		t.Fatal(err)
	}
	var out NativeBundle
	if err := json.Unmarshal([]byte(exported), &out); err != nil {
		t.Fatalf("native export is not valid JSON: %v\n%s", err, exported)
	}
	var raw map[string]any
	if err := json.Unmarshal([]byte(exported), &raw); err != nil {
		t.Fatalf("native export is not valid JSON object: %v\n%s", err, exported)
	}
	if len(out.Contacts) != 1 ||
		out.Contacts[0].MessageCount != 99 ||
		out.Contacts[0].ParticipantCount != 123 ||
		strings.Contains(exported, "message_digest") ||
		containsJSONKey(raw, "message_digest") {
		t.Fatalf("native export did not preserve rollup-only shape:\n%s", exported)
	}
}

func TestNativeImportRejectsUnsupportedSchemaVersion(t *testing.T) {
	bundle := NativeBundle{
		SchemaVersion: contracts.SchemaVersionPhase00 + 1,
		Contacts: []NativeContact{{
			ContactID: "https://example.test/#alice",
		}},
	}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}

	_, _, err = BuildImportObject(
		FormatNative,
		"contacts",
		"alice.json",
		encoded,
		time.Time{},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "unsupported native contact schema_version") {
		t.Fatalf("expected unsupported schema_version error, got %v", err)
	}
}

func TestVCardImportHandlesGroupsFoldingAndAnonymousSubjects(t *testing.T) {
	object, result, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"anonymous.vcf",
		[]byte(strings.Join([]string{
			"BEGIN:VCARD",
			"VERSION:4.0",
			"UID:first",
			"FN:Same Name",
			"item1.EMAIL:first@example.test",
			"NOTE:first line",
			"  indented",
			"END:VCARD",
			"BEGIN:VCARD",
			"VERSION:4.0",
			"UID:second",
			"FN:Same Name",
			"TEL:+15551234567",
			"END:VCARD",
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contacts) != 2 || result.Contacts[0] == result.Contacts[1] {
		t.Fatalf("expected two distinct contacts, got %#v", result.Contacts)
	}
	for _, snippet := range []string{
		"<mailto:first@example.test>",
		"\"first line indented\"",
		"\"+15551234567\"",
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("imported RDF missing %q:\n%s", snippet, object.Content)
		}
	}
}

func TestFOAFExportEscapesIRIsAndKeepsBlankNodes(t *testing.T) {
	output := ExportFOAF([]contracts.ContactAggregate{{
		ContactID: "<https://example.test/#alice>",
		Facts: []contracts.ContactFact{
			{
				ContactID: "https://example.test/#alice",
				FactKind:  contactentity.FactKindURL,
				Value:     "https://example.test/a b>c",
			},
			{
				ContactID: "https://example.test/#alice",
				FactKind:  contactentity.FactKindRelationship,
				Value:     "_:friend1",
			},
		},
	}})

	for _, snippet := range []string{
		"<https://example.test/#alice>",
		"<https://example.test/a%20b%3Ec>",
		"_:friend1",
	} {
		if !strings.Contains(output, snippet) {
			t.Fatalf("FOAF export missing %q:\n%s", snippet, output)
		}
	}
	if strings.Contains(output, "<<https://example.test/#alice>>") ||
		strings.Contains(output, "<_:friend1>") ||
		strings.Contains(output, "<https://example.test/a b>c>") {
		t.Fatalf("FOAF export emitted malformed IRI/blank node:\n%s", output)
	}
}

func containsJSONKey(value any, key string) bool {
	switch typed := value.(type) {
	case map[string]any:
		for k, v := range typed {
			if k == key || containsJSONKey(v, key) {
				return true
			}
		}
	case []any:
		for _, item := range typed {
			if containsJSONKey(item, key) {
				return true
			}
		}
	}

	return false
}

func TestStandardExportsOmitMessageBackReferences(t *testing.T) {
	aggregate := contracts.ContactAggregate{
		ContactID:    "https://example.test/#alice",
		DisplayName:  "Alice Example",
		PrimaryEmail: "alice@example.test",
		Facts: []contracts.ContactFact{{
			ContactID: "https://example.test/#alice",
			FactKind:  contactentity.FactKindEmail,
			Value:     "alice@example.test",
		}},
		MessageCount:     1000000,
		ParticipantCount: 2000000,
	}

	for _, output := range []string{ExportVCard([]contracts.ContactAggregate{aggregate}), ExportFOAF([]contracts.ContactAggregate{aggregate})} {
		if strings.Contains(output, "message_digest") ||
			strings.Contains(output, "1000000") ||
			strings.Contains(output, "2000000") {
			t.Fatalf("standard export leaked high-cardinality rollups:\n%s", output)
		}
	}
}
