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
	if object.MediaType != "text/turtle" ||
		object.SourceKind != contactImportSourceKind ||
		len(object.Facets) != 2 ||
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
	if !strings.Contains(exported, `"message_count": 99`) ||
		!strings.Contains(exported, `"participant_count": 123`) ||
		strings.Contains(exported, "message_digest") {
		t.Fatalf("native export did not preserve rollup-only shape:\n%s", exported)
	}
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
