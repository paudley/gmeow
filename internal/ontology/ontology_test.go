// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

import "testing"

func TestByIRIGrounding(t *testing.T) {
	cases := []struct {
		iri     string
		concept string
		kind    Kind
		role    Role
	}{
		{Schema + "email", "email", Set, RoleLocator},
		{Schema + "telephone", "phone", Set, RoleLocator},
		{Schema + "url", "url", Set, RoleLocator},
		{Schema + "name", "name", Name, RoleNone},
		{Schema + "givenName", "given-name", Name, RoleNone},
		{FullName, "name", Name, RoleNone},
		{PartText, "name-part", Name, RoleNone},
		{Schema + "familyName", "family-name", Name, RoleNone},
		{FOAF + "account", "account", Set, RoleAccount},
		{Schema + "gender", "gender", Functional, RoleNone},
	}
	for _, c := range cases {
		term, ok := ByIRI(c.iri)
		if !ok {
			t.Fatalf("%s: not in registry", c.iri)
		}
		if term.Concept != c.concept || term.Kind != c.kind || term.Role != c.role {
			t.Fatalf("%s: got concept=%q kind=%d role=%d, want %q/%d/%d",
				c.iri, term.Concept, term.Kind, term.Role, c.concept, c.kind, c.role)
		}
	}

	if _, ok := ByIRI(Schema + "contactPoint"); ok {
		t.Fatalf("structural predicate schema:contactPoint must NOT be a comparison term")
	}
}

func TestRegistryInvariants(t *testing.T) {
	for _, term := range Terms() {
		if term.Concept == "" {
			t.Fatalf("%s: registry terms must carry a concept", term.IRI)
		}
		if term.Normalize == nil {
			t.Fatalf("%s: registry terms must carry a Normalize func", term.IRI)
		}
		// Standards-first: a gmeow: term MUST justify itself; a standard term must NOT.
		if term.IsGmeow() && term.GmeowReason == "" {
			t.Fatalf("%s: gmeow: term lacks a GmeowReason", term.IRI)
		}
		if !term.IsGmeow() && term.GmeowReason != "" {
			t.Fatalf("%s: standard term must not carry a GmeowReason", term.IRI)
		}
	}
}

func TestValueNormalizers(t *testing.T) {
	cases := []struct {
		fn       func(string) string
		in, want string
	}{
		{NormEmail, "Patrick Audley <PAUDLEY@Blackcat.CA>", "paudley@blackcat.ca"},
		{NormEmail, "mailto:Paudley@Example.COM", "paudley@example.com"},
		{NormEmail, "  plain@x.com ", "plain@x.com"},
		{NormPhone, "+1 (403) 555-0123", "4035550123"},
		{NormPhone, "tel:403.555.0123", "4035550123"},
		{NormPhone, "5550123", "5550123"},
		// Degenerate "phones" carry no identity and must be rejected (→ "") so they
		// never seed the identifier index (corpus: "phone 0" pooled 82 entities).
		{NormPhone, "0", ""},
		{NormPhone, "2", ""},
		{NormPhone, "n/a", ""},
		{NormPhone, "555-12", ""}, // 5 digits, below the subscriber-number minimum
		// Backslash-corrupted URLs collapse onto the clean form (corpus: one site
		// fractured into http%5c://, http&#92;//, and the clean URL).
		{NormURL, "HTTP://Example.com/", "http://example.com"},
		{NormURL, "http%5c://www.cambrianhouse.com", "http://www.cambrianhouse.com"},
		{NormURL, "http://www.cambrianhouse.com", "http://www.cambrianhouse.com"},
		{NormText, "  Patrick   AUDLEY ", "patrick audley"},
	}
	for _, c := range cases {
		if got := c.fn(c.in); got != c.want {
			t.Fatalf("normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestIMAccountFromPseudoEmail(t *testing.T) {
	cases := []struct {
		in       string
		want     string
		wantIsIM bool
	}{
		{
			"bill.trembley%5c40gmail.com@msn.i.blackcat.ca",
			"msn:bill.trembley@gmail.com",
			true,
		},
		{
			"-668103306&#92;40chat.facebook.com@fb.i.blackcat.ca",
			"fb:-668103306@chat.facebook.com",
			true,
		},
		{"102998083@icq.i.blackcat.ca", "icq:102998083", true},
		// Ordinary emails are not IM accounts.
		{"paudley@blackcat.ca", "", false},
		{"someone@example.com", "", false},
	}
	for _, c := range cases {
		got, ok := IMAccountFromPseudoEmail(c.in)
		if ok != c.wantIsIM || (ok && got != c.want) {
			t.Fatalf(
				"IMAccountFromPseudoEmail(%q) = (%q,%v), want (%q,%v)",
				c.in,
				got,
				ok,
				c.want,
				c.wantIsIM,
			)
		}
	}
}
