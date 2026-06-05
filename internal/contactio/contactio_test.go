// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/micromdm/plist"

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
		// Canonical node structure: email is a schema:ContactPoint keyed by mailto:,
		// name/org are standards-first (schema:name / schema:worksFor).
		"<https://schema.org/contactPoint> <mailto:alice@example.test>",
		"<mailto:alice@example.test> <https://schema.org/email> <mailto:alice@example.test>",
		// Names are a reified gmeow:PersonName appellation, never a bare property.
		"<https://blackcatinformatics.ca/gmeow/hasName>",
		"<https://blackcatinformatics.ca/gmeow/PersonName>",
		"<https://blackcatinformatics.ca/gmeow/fullName> \"Alice Example\"",
		"<https://schema.org/worksFor> \"Example Org\"",
		"<https://schema.org/description> \"synthetic only\"",
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("imported RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	// Subject is now a stable OBSERVATION identity (not the email — emails are
	// temporal/transferable); the email is a schema:ContactPoint node.
	if object.MediaType != MediaTypeTurtle ||
		object.SourceKind != contactImportSourceKind ||
		len(object.Facets) != 1 ||
		object.Facets[0].Kind != contracts.RDFSourceBundleFacetKind ||
		len(result.Contacts) != 1 ||
		!strings.HasPrefix(result.Contacts[0], "urn:gmeow:observation:") {
		t.Fatalf("unexpected import object=%#v result=%#v", object, result)
	}
}

// TestVCardLocationEmitsGmeowModel: vCard ADR/GEO/TZ become the GMEOW-primary
// location model — a gmeow:PostalAddress with all seven components, a gmeow:Place
// carrying latitude/longitude, and gmeow:timezone — not schema:PostalAddress or a
// dropped vcardGeoParameter source predicate.
func TestVCardLocationEmitsGmeowModel(t *testing.T) {
	vcf := "BEGIN:VCARD\nVERSION:4.0\nFN:Pat Audley\n" +
		"ADR:POBox 9;Suite 5;112 Westbourne Rd;Spruce Grove;AB;T7X 0A1;CA\n" +
		"GEO:geo:53.544972,-113.924398\nTZ:America/Edmonton\nEND:VCARD\n"
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"pat.vcf",
		[]byte(vcf),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	g := "https://blackcatinformatics.ca/gmeow/"
	for _, want := range []string{
		"<" + g + "PostalAddress>",
		"<" + g + `postOfficeBox> "POBox 9"`,
		"<" + g + `extendedAddress> "Suite 5"`,
		"<" + g + `streetAddress> "112 Westbourne Rd"`,
		"<" + g + `addressLocality> "Spruce Grove"`,
		"<" + g + `addressRegion> "AB"`,
		"<" + g + `postalCode> "T7X 0A1"`,
		"<" + g + `countryCode> "CA"`,
		"<" + g + "Place>",
		"<" + g + `latitude> "53.544972"`,
		"<" + g + `longitude> "-113.924398"`,
		"<" + g + `timezone> "America/Edmonton"`,
	} {
		if !strings.Contains(object.Content, want) {
			t.Fatalf("location RDF missing %q:\n%s", want, object.Content)
		}
	}
	for _, bad := range []string{"schema.org/PostalAddress", "vcardGeoParameter", "vcardTimeZone"} {
		if strings.Contains(object.Content, bad) {
			t.Fatalf("location RDF still emits legacy %q:\n%s", bad, object.Content)
		}
	}
}

func TestVCardImportPreservesInvalidEmailValuesAsSourceData(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"bad-email.vcf",
		[]byte(
			"BEGIN:VCARD\nVERSION:4.0\nFN:Phone Only\nEMAIL:+1 (604) 555-1212\nTEL:+1 (604) 555-1212\nEND:VCARD\n",
		),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(object.Content, "mailto:+1") {
		t.Fatalf(
			"invalid EMAIL value was emitted as canonical mailto RDF:\n%s",
			object.Content,
		)
	}
	// A junk EMAIL (no @) is preserved as evidence — a schema:email LITERAL, not a
	// coerced mailto: node.
	if !strings.Contains(
		object.Content,
		`<https://schema.org/email> "+1 (604) 555-1212"`,
	) {
		t.Fatalf("invalid EMAIL source value was not preserved:\n%s", object.Content)
	}
}

func TestVCardImportScopesProviderLifecycleToIMEndpoints(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"legacy-im.vcf",
		[]byte(strings.Join([]string{
			"BEGIN:VCARD",
			"VERSION:4.0",
			"FN:Legacy IM",
			"EMAIL:user@aim.com",
			"X-AIM:synthetic-screen-name",
			"X-ICQ:123456",
			"MSN:synthetic-msn",
			"X-JABBER:synthetic@example.test",
			"X-MYSPACE:https://example.test/legacy-profile",
			"END:VCARD",
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	// IM/social identities are foaf:OnlineAccount nodes (one pattern). Dead
	// services carry a time:hasEnd lifecycle bound on the account node.
	for _, snippet := range []string{
		`<http://xmlns.com/foaf/0.1/accountName> "synthetic-screen-name"`,
		`<http://www.w3.org/2006/time#hasEnd> "2017-12-15"^^<http://www.w3.org/2001/XMLSchema#date>`,
		`<http://xmlns.com/foaf/0.1/accountName> "123456"`,
		`<http://www.w3.org/2006/time#hasEnd> "2024-06-26"^^<http://www.w3.org/2001/XMLSchema#date>`,
		`<http://xmlns.com/foaf/0.1/accountName> "synthetic-msn"`,
		`<http://www.w3.org/2006/time#hasEnd> "2013-04-30"^^<http://www.w3.org/2001/XMLSchema#date>`,
		`<http://xmlns.com/foaf/0.1/OnlineAccount>`,
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("provider lifecycle RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	// Lifecycle is scoped to the dead-service endpoints only: AIM/ICQ/MSN get a
	// hasEnd; the live-ish Jabber/MySpace accounts do not — so exactly three.
	if got := strings.Count(
		object.Content,
		"<http://www.w3.org/2006/time#hasEnd>",
	); got != 3 {
		t.Fatalf(
			"want 3 lifecycle hasEnd bounds (aim/icq/msn), got %d:\n%s",
			got,
			object.Content,
		)
	}
}

func TestVCardImportSupportsQuotedPrintableContinuationAndNameExtensions(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"quoted-printable.vcf",
		[]byte(strings.Join([]string{
			"BEGIN:VCARD",
			"VERSION:3.0",
			"FN:Encoded Example",
			"NOTE;ENCODING=QUOTED-PRINTABLE:first=0A=",
			"second",
			"X-MAIDENNAME:Former Synthetic",
			"X-PHONETIC-LAST-NAME:synthetic-pronunciation",
			"X-FC-STASH-GDATA-123:stash-token",
			"X-FC-GOOGLE-URI-X-ABSHOWAS:google-uri-token",
			"[ref]=0A[ref]=0ACATEGORIES:legacy-category-fragment",
			"@friend @tracked=0A=0AE-mail Address:legacy-email-label-fragment",
			"@friend @tracked=0A=0AIM:legacy-im-label-fragment",
			"=0AIM:legacy-im-fragment",
			"ESS:legacy-continuation-fragment",
			"X-FC-TAGS:tag-token",
			"END:VCARD",
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, snippet := range []string{
		"first\\nsecond",
		// Standalone vendor name parts (X-MAIDENNAME / X-PHONETIC) ground via the
		// schema:familyName alias (a flat lexical part outside the assembled FN/N
		// appellation); it still tokenizes for comparison. The long-tail fastmail /
		// legacy-fragment metadata is preserved under its source-property predicate.
		`<https://schema.org/familyName> "Former Synthetic"`,
		`<https://schema.org/familyName> "synthetic-pronunciation"`,
		`<https://blackcatinformatics.ca/gmeow/fastmailGDataStash> "stash-token"`,
		`<https://blackcatinformatics.ca/gmeow/fastmailGoogleURI> "google-uri-token"`,
		`<https://blackcatinformatics.ca/gmeow/legacyEncodedCategoryFragment> "legacy-category-fragment"`,
		`<https://blackcatinformatics.ca/gmeow/legacyEncodedEmailLabelFragment> "legacy-email-label-fragment"`,
		`<https://blackcatinformatics.ca/gmeow/legacyEncodedIMLabelFragment> "legacy-im-label-fragment"`,
		`<https://blackcatinformatics.ca/gmeow/legacyEncodedIMLabelFragment> "legacy-im-fragment"`,
		`<https://blackcatinformatics.ca/gmeow/legacyEncodedContinuationFragment> "legacy-continuation-fragment"`,
		`<https://blackcatinformatics.ca/gmeow/fastmailTags> "tag-token"`,
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("vCard RDF missing %q:\n%s", snippet, object.Content)
		}
	}
}

func TestBBDBImportScopesProviderLifecycleToIMEndpoints(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatBBDB,
		"contacts",
		".bbdb",
		[]byte(
			`["Legacy" "Messenger" nil nil nil nil ("legacy@example.test") ((yahoo . "synthetic-yahoo") (skype . "synthetic-skype")) nil]`,
		),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, snippet := range []string{
		`<https://blackcatinformatics.ca/gmeow/yahooIdentity> "synthetic-yahoo"`,
		`<http://www.w3.org/2006/time#hasEnd> "2018-07-17"^^<http://www.w3.org/2001/XMLSchema#date>`,
		`<https://blackcatinformatics.ca/gmeow/skypeIdentity> "synthetic-skype"`,
		`<http://www.w3.org/2006/time#hasEnd> "2025-05-05"^^<http://www.w3.org/2001/XMLSchema#date>`,
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("BBDB provider lifecycle RDF missing %q:\n%s", snippet, object.Content)
		}
	}
}

func TestProviderLifecycleTableCarriesSourceMetadata(t *testing.T) {
	for predicate, lifecycle := range providerLifecycleByPredicate {
		if lifecycle.ValidUntil == "" ||
			lifecycle.Confidence == "" ||
			lifecycle.SourceURL == "" ||
			lifecycle.Caveat == "" {
			t.Fatalf("provider lifecycle %q is missing metadata: %#v", predicate, lifecycle)
		}
	}
	if _, found := providerLifecycleByPredicate[gmeowPrefix+"jabberIdentity"]; found {
		t.Fatal("generic Jabber/XMPP must not have a global lifecycle shutdown date")
	}
	if _, found := providerLifecycleByPredicate[gmeowPrefix+"myspaceProfile"]; found {
		t.Fatal("MySpace profiles must not have a global lifecycle shutdown date")
	}
}

func TestVCardImportRejectsUnsupportedPropertiesAndParameters(t *testing.T) {
	// An unmapped property/parameter rejects the affected RECORD (per-record
	// boundary) rather than failing the file. X- vendor extensions are mapped by
	// rule (see TestVCardImportMapsVendorExtensionProperties); these use a non-X-
	// unmapped property and an unmapped parameter.
	for name, input := range map[string]string{
		"property":  "BEGIN:VCARD\nVERSION:4.0\nFN:Alice\nNOTAREALPROPERTY:value\nEND:VCARD\n",
		"parameter": "BEGIN:VCARD\nVERSION:4.0\nFN:Alice\nEMAIL;X-NOT-MAPPED=home:alice@example.test\nEND:VCARD\n",
	} {
		t.Run(name, func(t *testing.T) {
			_, result, err := BuildImportObject(
				FormatVCard,
				"contacts",
				"unsupported.vcf",
				[]byte(input),
				time.Time{},
			)
			if err != nil {
				t.Fatalf("expected per-record rejection, got file error %v", err)
			}
			if len(result.Contacts) != 0 {
				t.Fatalf(
					"expected the single bad record rejected, got contacts %v",
					result.Contacts,
				)
			}
			if len(result.Rejected) != 1 {
				t.Fatalf(
					"expected 1 rejected record, got %d (%v)",
					len(result.Rejected),
					result.Rejected,
				)
			}
		})
	}
}

func TestVCardImportRejectsOnlyTheBadRecord(t *testing.T) {
	// A multi-card file with one damaged record imports the good cards and
	// rejects only the bad one.
	input := "BEGIN:VCARD\nVERSION:4.0\nFN:Good One\nEMAIL:good@example.test\nEND:VCARD\n" +
		"BEGIN:VCARD\nVERSION:4.0\nFN:Bad One\nNOTAREALPROPERTY:boom\nEND:VCARD\n" +
		"BEGIN:VCARD\nVERSION:4.0\nFN:Good Two\nEND:VCARD\n"
	object, result, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"mixed.vcf",
		[]byte(input),
		time.Time{},
	)
	if err != nil {
		t.Fatalf("multi-card import failed at file level: %v", err)
	}
	if len(result.Contacts) != 2 {
		t.Fatalf(
			"expected 2 imported contacts, got %d (%v)",
			len(result.Contacts),
			result.Contacts,
		)
	}
	if len(result.Rejected) != 1 || result.Rejected[0].Index != 2 {
		t.Fatalf("expected record 2 rejected, got %v", result.Rejected)
	}
	if !strings.Contains(object.Content, `"Good One"`) ||
		!strings.Contains(object.Content, `"Good Two"`) {
		t.Fatalf("good records missing from output:\n%s", object.Content)
	}
	if strings.Contains(object.Content, "Bad One") {
		t.Fatalf("rejected record leaked into output:\n%s", object.Content)
	}
}

func TestBuildContactDeltasEmitsOneObjectPerContact(t *testing.T) {
	input := "BEGIN:VCARD\nVERSION:4.0\nFN:Alice One\nEMAIL:alice@example.test\nEND:VCARD\n" +
		"BEGIN:VCARD\nVERSION:4.0\nFN:Bob Two\nEMAIL:bob@example.test\nEND:VCARD\n"
	deltas, result, err := BuildContactDeltas(
		FormatVCard,
		"corpus",
		[]byte(input),
		ImportOptions{ImportLevel: 5},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(deltas) != 2 {
		t.Fatalf("expected one delta per contact, got %d", len(deltas))
	}
	if len(result.Contacts) != 2 {
		t.Fatalf("expected 2 contacts in result, got %v", result.Contacts)
	}
	for _, delta := range deltas {
		// Each delta is a standalone Turtle doc: prefixes + the contact + its import level.
		if !strings.Contains(delta.Content, "@prefix gmeow:") {
			t.Fatalf("delta missing prefixes:\n%s", delta.Content)
		}
		if !strings.Contains(
			delta.Content,
			"<"+delta.Identity+"> <"+GmeowImportanceLevel+"> \"5\"",
		) {
			t.Fatalf(
				"delta %q missing its own import-level claim:\n%s",
				delta.Identity,
				delta.Content,
			)
		}
	}
	if deltas[0].Identity == deltas[1].Identity {
		t.Fatalf("deltas should have distinct identities: %v", deltas)
	}
}

func TestBuildContactDeltasStableForDedup(t *testing.T) {
	// The same contact observed in two different files must render identical
	// delta content (so the source-object index dedups it across snapshots).
	card := "BEGIN:VCARD\nVERSION:4.0\nFN:Stable Person\nEMAIL:stable@example.test\nEND:VCARD\n"
	a, _, err := BuildContactDeltas(
		FormatVCard,
		"corpus",
		[]byte(card),
		ImportOptions{ImportLevel: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	b, _, err := BuildContactDeltas(
		FormatVCard,
		"corpus",
		[]byte(card),
		ImportOptions{ImportLevel: 3},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(a) != 1 || len(b) != 1 {
		t.Fatalf("expected single delta each, got %d and %d", len(a), len(b))
	}
	if a[0].Identity != b[0].Identity || a[0].Content != b[0].Content {
		t.Fatalf("same contact must render identically for dedup:\n%q\nvs\n%q", a[0], b[0])
	}
}

func TestVCardImportClassifiesLocatorOnlyVsAgent(t *testing.T) {
	personType := "<" + foafPrefix + "Person>"

	agent := mustVCardContent(
		t,
		"BEGIN:VCARD\nVERSION:4.0\nFN:Alice Agent\nEMAIL:alice@example.test\nEND:VCARD\n",
	)
	if !strings.Contains(agent, personType) {
		t.Fatalf("named card should be a foaf:Person:\n%s", agent)
	}

	for name, input := range map[string]string{
		"email-only": "BEGIN:VCARD\nVERSION:4.0\nFN:\nN:;;;;\nEMAIL:lonely@example.test\nEND:VCARD\n",
		"phone-only": "BEGIN:VCARD\nVERSION:4.0\nFN:\nN:;;;;\nTEL:+1 555 0100\nEND:VCARD\n",
		"url-only":   "BEGIN:VCARD\nVERSION:4.0\nFN:\nN:;;;;\nURL:https://example.test/p\nEND:VCARD\n",
	} {
		t.Run(name, func(t *testing.T) {
			content := mustVCardContent(t, input)
			if strings.Contains(content, personType) {
				t.Fatalf("locator-only card must NOT create a foaf:Person:\n%s", content)
			}
		})
	}

	org := mustVCardContent(
		t,
		"BEGIN:VCARD\nVERSION:4.0\nFN:\nN:;;;;\nORG:Acme Inc\nTEL:+1 555 0199\nEND:VCARD\n",
	)
	if !strings.Contains(org, personType) {
		t.Fatalf("org-bearing card should be agent-denoting:\n%s", org)
	}
}

func mustVCardContent(t *testing.T, input string) string {
	t.Helper()
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"classify.vcf",
		[]byte(input),
		time.Time{},
	)
	if err != nil {
		t.Fatalf("import failed: %v", err)
	}

	return object.Content
}

func TestVCardImportMapsVendorExtensionProperties(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"vendor-ext.vcf",
		[]byte(
			"BEGIN:VCARD\nVERSION:4.0\nFN:Alice\nX-ACME-LOYALTY-ID:42\nX-SINGLESEG:on\nEND:VCARD\n",
		),
		time.Time{},
	)
	if err != nil {
		t.Fatalf("vendor-extension vCard rejected: %v", err)
	}
	// X-<vendor>-<name> → gmeow:vendorExtension/<vendor>/<name>; single-segment → /<name>.
	if !strings.Contains(object.Content, gmeowPrefix+"vendorExtension/ACME/LOYALTY-ID") {
		t.Fatalf("missing vendor/name extension predicate:\n%s", object.Content)
	}
	if !strings.Contains(object.Content, gmeowPrefix+"vendorExtension/SINGLESEG") {
		t.Fatalf("missing single-segment extension predicate:\n%s", object.Content)
	}
	if !strings.Contains(object.Content, `"42"`) ||
		!strings.Contains(object.Content, `"on"`) {
		t.Fatalf("vendor-extension values not preserved as literals:\n%s", object.Content)
	}
}

func TestVCardImportNormalizesInvalidUTF8(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatVCard,
		"contacts",
		"invalid-utf8.vcf",
		[]byte{
			'B', 'E', 'G', 'I', 'N', ':', 'V', 'C', 'A', 'R', 'D', '\n',
			'V', 'E', 'R', 'S', 'I', 'O', 'N', ':', '4', '.', '0', '\n',
			'F', 'N', ':', 'I', 'n', 'v', 'a', 'l', 'i', 'd', '\n',
			'E', 'M', 'A', 'I', 'L', ':', 'i', 'n', 'v', 'a', 'l', 'i', 'd',
			'@', 'e', 'x', 'a', 'm', 'p', 'l', 'e', '.', 't', 'e', 's', 't', '\n',
			'N', 'O', 'T', 'E', ':', 'b', 'a', 'd', ' ', 0x8d, ' ', 'b', 'y',
			't', 'e', '\n',
			'E', 'N', 'D', ':', 'V', 'C', 'A', 'R', 'D', '\n',
		},
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !utf8.ValidString(object.Content) ||
		strings.Contains(object.Content, string([]byte{0x8d})) {
		t.Fatalf("imported RDF contains invalid UTF-8:\n%q", object.Content)
	}
	if !strings.Contains(object.Content, `"bad ? byte"`) {
		t.Fatalf("invalid UTF-8 was not replaced in RDF literal:\n%s", object.Content)
	}
}

func TestRDFImportRootedGraphIsOneContact(t *testing.T) {
	// A graph declaring schema:mainEntity is ONE rooted contact; embedded
	// people/orgs/individuals are that contact's claims, not separate contacts.
	graph := `@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .
@prefix gedcom: <http://www.w3.org/2000/10/swap/pim/gedcom#> .
<https://example.test/#page> a schema:WebPage ;
    schema:mainEntity <https://example.test/#me> .
<https://example.test/#me> a foaf:Person, schema:Person ;
    schema:name "Root Person" ;
    foaf:knows <https://example.test/#friend> .
<https://example.test/#friend> a foaf:Person ;
    schema:name "Embedded Friend" .
<https://example.test/#ancestor> a gedcom:Individual ;
    schema:name "Embedded Ancestor" .
`
	object, result, err := BuildImportObject(
		FormatRDF,
		"rooted",
		"profile.ttl",
		[]byte(graph),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contacts) != 1 || result.Contacts[0] != "https://example.test/#me" {
		t.Fatalf("rooted graph must be exactly one contact (#me), got %v", result.Contacts)
	}
	// Semantic superset: every input subject's triples are preserved in the bundle.
	for _, snippet := range []string{"#me", "#friend", "#ancestor", "Embedded Ancestor"} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("rooted graph dropped %q (superset violated):\n%s", snippet, object.Content)
		}
	}
}

func TestRDFImportUnrootedCollectionYieldsManyContacts(t *testing.T) {
	// No primary-subject declaration => an un-rooted collection of co-equal
	// agents, each its own contact.
	graph := `@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix schema: <https://schema.org/> .

<https://example.test/#a> a foaf:Person ;
    schema:name "Alice" .

<https://example.test/#b> a foaf:Person ;
    schema:name "Bob" .
`
	_, result, err := BuildImportObject(
		FormatRDF,
		"collection",
		"people.ttl",
		[]byte(graph),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contacts) != 2 {
		t.Fatalf("un-rooted collection should yield 2 contacts, got %v", result.Contacts)
	}
}

func TestRDFImportRejectsBundlesWithoutContacts(t *testing.T) {
	_, _, err := BuildImportObject(
		FormatRDF,
		"contacts",
		"not-contacts.ttl",
		[]byte("Name,Email\nAlice,alice@example.test\n"),
		time.Time{},
	)
	if err == nil || !strings.Contains(err.Error(), "RDF/Turtle import has no contacts") {
		t.Fatalf("BuildImportObject error = %v, want no contacts error", err)
	}
}

func TestRDFImportUsesTopLevelContactSubjects(t *testing.T) {
	object, result, err := BuildImportObjectWithOptions(
		FormatRDF,
		"synthetic-contacts",
		"single-entry.ttl",
		[]byte(`@prefix foaf: <http://xmlns.com/foaf/0.1/> .
@prefix rel: <http://purl.org/vocab/relationship/> .
@prefix schema: <https://schema.org/> .

<https://example.test/#root> a foaf:Person, schema:Person ;
    foaf:name "Root Contact" ;
    rel:parentOf <https://example.test/#related> .

<https://example.test/#related> a foaf:Person, schema:Person ;
    foaf:name "Related Contact" .
`),
		time.Date(2026, 6, 1, 12, 0, 0, 0, time.UTC),
		ImportOptions{ImportLevel: 4},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(result.Contacts) != 1 || result.Contacts[0] != "https://example.test/#root" {
		t.Fatalf("contacts = %#v, want only top-level contact", result.Contacts)
	}
	if strings.Contains(
		object.Content,
		"<https://example.test/#related> <https://blackcatinformatics.ca/gmeow/importanceLevel>",
	) {
		t.Fatalf("nested related contact received source import level:\n%s", object.Content)
	}
}

func TestAppleAddressBookImportProducesRDFContactBundle(t *testing.T) {
	content := mustPlist(t, map[string]any{
		"UID":           "SYNTHETIC-APPLE-UID",
		"First":         "Casey",
		"Last":          "Contact",
		"Organization":  "Example Org",
		"JobTitle":      "Research Lead",
		"Creation":      time.Date(2020, 1, 2, 3, 4, 5, 0, time.UTC),
		"Modification":  time.Date(2021, 2, 3, 4, 5, 6, 0, time.UTC),
		"ABPersonFlags": int64(0),
		"Email": map[string]any{
			"identifiers": []any{"email-1"},
			"labels":      []any{"_$!<Work>!$_"},
			"primary":     "email-1",
			"values":      []any{"casey.contact@example.test"},
		},
		"Address": map[string]any{
			"identifiers": []any{"address-1"},
			"labels":      []any{"_$!<Work>!$_"},
			"primary":     "address-1",
			"values": []any{map[string]any{
				"City":    "Example City",
				"Country": "Example Country",
				"State":   "Example State",
				"Street":  "1 Example Street",
				"ZIP":     "A1A 1A1",
			}},
		},
		"ABPropertyTypes": map[string]any{
			"UID":   int64(0),
			"Email": int64(4),
		},
	})
	object, result, err := BuildImportObject(
		FormatAppleAddressBook,
		"contacts",
		"synthetic.abcdp",
		content,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, snippet := range []string{
		"<mailto:casey.contact@example.test>",
		`<https://blackcatinformatics.ca/gmeow/PersonName>`,
		`<https://blackcatinformatics.ca/gmeow/partText> "Casey"`,
		`<https://blackcatinformatics.ca/gmeow/namePartType> <https://blackcatinformatics.ca/gmeow/namePartGiven>`,
		`<https://schema.org/affiliation> "Example Org"`,
		`<https://schema.org/email> <mailto:casey.contact@example.test>`,
		`<https://blackcatinformatics.ca/gmeow/streetAddress> "1 Example Street"`,
		`<https://blackcatinformatics.ca/gmeow/addressLocality> "Example City"`,
		`<https://blackcatinformatics.ca/gmeow/countryCode> "Example Country"`,
		`<https://blackcatinformatics.ca/gmeow/applePropertyTypeName> "Email"`,
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("Apple AddressBook RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	if len(result.Contacts) != 1 ||
		result.Contacts[0] != "mailto:casey.contact@example.test" {
		t.Fatalf("unexpected Apple AddressBook import result: %#v", result)
	}
}

func TestAppleAddressBookGroupImportProducesGroupAndMembers(t *testing.T) {
	content := mustPlist(t, map[string]any{
		"UID":             "GROUP-UID:ABGroup",
		"ABGroupClassKey": "ABGroup",
		"GroupName":       "Project Team",
		"ABMembers":       []any{"PERSON-A:ABPerson", "PERSON-B:ABPerson"},
		"ABEmailDistributionList": map[string]any{
			"PERSON-A:ABPerson": "email-id-1",
		},
	})
	object, result, err := BuildImportObject(
		FormatAppleAddressBookGroup,
		"contacts",
		"synthetic.abcdg",
		content,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, snippet := range []string{
		"<" + foafPrefix + "Group>",
		`<https://schema.org/name> "Project Team"`,
		"<" + foafPrefix + "member> <urn:gmeow:contact:apple-addressbook:PERSON-A:ABPerson>",
		"<" + foafPrefix + "member> <urn:gmeow:contact:apple-addressbook:PERSON-B:ABPerson>",
		gmeowPrefix + "appleEmailDistributionList",
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("Apple group RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	if len(result.Contacts) != 1 {
		t.Fatalf("expected 1 group contact, got %#v", result.Contacts)
	}
}

func TestAppleAddressBookGroupImportRejectsUnknownField(t *testing.T) {
	content := mustPlist(t, map[string]any{
		"UID":          "GROUP-UID:ABGroup",
		"GroupName":    "Team",
		"MysteryField": "boom",
	})
	if _, _, err := BuildImportObject(
		FormatAppleAddressBookGroup,
		"contacts",
		"bad.abcdg",
		content,
		time.Time{},
	); err == nil {
		t.Fatal("expected rejection of unknown Apple group field")
	}
}

func TestAppleAddressBookImportRejectsUnsupportedFields(t *testing.T) {
	_, _, err := BuildImportObject(
		FormatAppleAddressBook,
		"contacts",
		"synthetic.abcdp",
		mustPlist(t, map[string]any{
			"UID":             "SYNTHETIC-APPLE-UID",
			"UnsupportedKey":  "value",
			"ABPropertyTypes": map[string]any{"UID": int64(0)},
		}),
		time.Time{},
	)
	if err == nil ||
		!strings.Contains(err.Error(), "unsupported Apple AddressBook person field") {
		t.Fatalf("expected unsupported Apple AddressBook field rejection, got %v", err)
	}
}

func TestCSVImportSupportsLinkedInConnections(t *testing.T) {
	object, result, err := BuildImportObject(
		FormatCSV,
		"contacts",
		"Connections.csv",
		[]byte(strings.Join([]string{
			"First Name,Last Name,Email Address,Current Company,Current Position",
			"Casey,Contact,casey.contact@example.test,Example Org,Research Lead",
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, snippet := range []string{
		"<mailto:casey.contact@example.test>",
		// Identity columns ground to canonical standard predicates (cross-dialect
		// convergence), not source-namespaced gmeow:linkedIn* terms.
		`<https://blackcatinformatics.ca/gmeow/partText> "Casey"`,
		`<https://blackcatinformatics.ca/gmeow/partText> "Contact"`,
		`<https://blackcatinformatics.ca/gmeow/namePartType> <https://blackcatinformatics.ca/gmeow/namePartSurname>`,
		`<https://schema.org/worksFor> "Example Org"`,
		`<https://schema.org/jobTitle> "Research Lead"`,
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("LinkedIn CSV RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	if len(result.Contacts) != 1 ||
		result.Contacts[0] != "mailto:casey.contact@example.test" {
		t.Fatalf("unexpected LinkedIn CSV result: %#v", result)
	}
}

func mustPlist(t *testing.T, value any) []byte {
	t.Helper()
	content, err := plist.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}

	return content
}

func TestCSVImportRejectsUnsupportedSchema(t *testing.T) {
	_, _, err := BuildImportObject(
		FormatCSV,
		"contacts",
		"unsupported.csv",
		[]byte("Unmapped,Also Unmapped\nvalue,other\n"),
		time.Time{},
	)
	if err == nil || !strings.Contains(err.Error(), "unsupported CSV contact schema") {
		t.Fatalf("expected unsupported CSV schema rejection, got %v", err)
	}
}

func TestCSVImportPreservesNonEmailEmailFields(t *testing.T) {
	object, _, err := BuildImportObject(
		FormatCSV,
		"contacts",
		"contacts.csv",
		[]byte(strings.Join([]string{
			"First Name,Last Name,Email",
			"Casey,Contact,not an email address",
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		object.Content,
		`<https://schema.org/email> "not an email address"`,
	) {
		t.Fatalf("non-email email field was not preserved as a literal:\n%s", object.Content)
	}
}

func TestGEDCOMImportProducesContactsAndRelationships(t *testing.T) {
	object, result, err := BuildImportObject(
		FormatGEDCOM,
		"family",
		"synthetic.ged",
		[]byte(strings.Join([]string{
			"0 HEAD",
			"1 SOUR Synthetic",
			"0 @I1@ INDI",
			"1 NAME Casey /Contact/",
			"1 EMAIL casey.contact@example.test",
			"1 BIRT",
			"2 DATE 1970-01-01",
			"0 @I2@ INDI",
			"1 NAME Riley /Contact/",
			"0 @F1@ FAM",
			"1 HUSB @I1@",
			"1 CHIL @I2@",
			"0 TRLR",
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, snippet := range []string{
		`<http://xmlns.com/foaf/0.1/name> "Casey /Contact/"`,
		`<https://schema.org/email> <mailto:casey.contact@example.test>`,
		`<http://purl.org/vocab/relationship/parentOf>`,
		`<http://purl.org/vocab/relationship/childOf>`,
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("GEDCOM RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	if len(result.Contacts) != 2 {
		t.Fatalf("unexpected GEDCOM contacts: %#v", result)
	}
}

func TestImportObjectWithOptionsAddsImportanceClaims(t *testing.T) {
	object, result, err := BuildImportObjectWithOptions(
		FormatVCard,
		"contacts",
		"alice.vcf",
		[]byte("BEGIN:VCARD\nVERSION:4.0\nFN:Alice\nEMAIL:alice@example.test\nEND:VCARD\n"),
		time.Time{},
		ImportOptions{ImportLevel: 7},
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.ImportLevel != 7 || object.ImportLevel != 7 {
		t.Fatalf(
			"importance level not carried through: object=%#v result=%#v",
			object,
			result,
		)
	}
	if !strings.Contains(
		object.Content,
		`<https://blackcatinformatics.ca/gmeow/importanceLevel> "7"^^<http://www.w3.org/2001/XMLSchema#integer>`,
	) {
		t.Fatalf("imported RDF missing importance claim:\n%s", object.Content)
	}
}

func TestImportObjectRejectsInvalidImportanceLevel(t *testing.T) {
	_, _, err := BuildImportObjectWithOptions(
		FormatVCard,
		"contacts",
		"alice.vcf",
		[]byte("BEGIN:VCARD\nVERSION:4.0\nFN:Alice\nEMAIL:alice@example.test\nEND:VCARD\n"),
		time.Time{},
		ImportOptions{ImportLevel: 11},
	)
	if err == nil || !strings.Contains(err.Error(), "contact import level") {
		t.Fatalf("expected import level validation error, got %v", err)
	}
}

func TestBBDBImportProducesRDFContactBundle(t *testing.T) {
	object, result, err := BuildImportObject(
		FormatBBDB,
		"contacts",
		".bbdb",
		[]byte(strings.Join([]string{
			";; -*-coding: iso-2022-7bit;-*-",
			";;; file-version: 6",
			`["Casey" "Contact" nil "Example Research Unit" nil nil ("casey.contact@example.test") ((country . "Canada")) nil]`,
			`["Blake" "Example" ("Example Household") nil nil nil ("blake.example@example.test") nil nil]`,
			"",
		}, "\n")),
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}

	for _, snippet := range []string{
		"<mailto:casey.contact@example.test>",
		"\"Casey Contact\"",
		"\"Example Research Unit\"",
		"\"Canada\"",
		"<mailto:blake.example@example.test>",
		"\"Example Household\"",
	} {
		if !strings.Contains(object.Content, snippet) {
			t.Fatalf("BBDB RDF missing %q:\n%s", snippet, object.Content)
		}
	}
	if len(result.Contacts) != 2 ||
		result.Contacts[0] != "mailto:casey.contact@example.test" ||
		result.Contacts[1] != "mailto:blake.example@example.test" {
		t.Fatalf("unexpected BBDB import result: %#v", result)
	}
}

func TestBBDBImportRejectsUnsupportedUserFields(t *testing.T) {
	_, _, err := BuildImportObject(
		FormatBBDB,
		"contacts",
		".bbdb",
		[]byte(
			`["Casey" "Contact" nil nil nil nil ("casey.contact@example.test") ((not-mapped . "value")) nil]`,
		),
		time.Time{},
	)
	if err == nil || !strings.Contains(err.Error(), "unsupported BBDB contact record") {
		t.Fatalf("expected unsupported BBDB rejection, got %v", err)
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
		"<https://blackcatinformatics.ca/gmeow/historicalEmail>",
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

func TestNativeImportAndExportPreservesContactAliases(t *testing.T) {
	bundle := NativeBundle{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Contacts: []NativeContact{{
			ContactID: "urn:gmeow:test:contact:fixture",
			Aliases:   []string{"Fixture Main"},
		}},
	}
	encoded, err := json.Marshal(bundle)
	if err != nil {
		t.Fatal(err)
	}

	object, _, err := BuildImportObject(
		FormatNative,
		"contacts",
		"fixture.json",
		encoded,
		time.Time{},
	)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(
		object.Content,
		"<https://blackcatinformatics.ca/gmeow/contactAlias> \"fixture-main\"",
	) {
		t.Fatalf("native RDF did not include contact alias:\n%s", object.Content)
	}

	exported, err := Export(FormatNative, []contracts.ContactAggregate{{
		ContactID: "urn:gmeow:test:contact:fixture",
		Aliases:   []string{"fixture-main"},
	}})
	if err != nil {
		t.Fatal(err)
	}
	var out NativeBundle
	if err := json.Unmarshal([]byte(exported), &out); err != nil {
		t.Fatalf("native export is not valid JSON: %v\n%s", err, exported)
	}
	if len(out.Contacts) != 1 ||
		len(out.Contacts[0].Aliases) != 1 ||
		out.Contacts[0].Aliases[0] != "fixture-main" {
		t.Fatalf("native export did not preserve aliases:\n%s", exported)
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
		// Phone is a canonical tel: ContactPoint node (E.164-normalized).
		"<tel:5551234567> <https://schema.org/telephone> <tel:5551234567>",
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
