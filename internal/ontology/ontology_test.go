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
		{Schema + "name", "name", Functional, RoleNone},
		{Schema + "givenName", "given-name", Contextual, RoleNone},
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
		{NormURL, "HTTP://Example.com/", "http://example.com"},
		{NormText, "  Patrick   AUDLEY ", "patrick audley"},
	}
	for _, c := range cases {
		if got := c.fn(c.in); got != c.want {
			t.Fatalf("normalize(%q) = %q, want %q", c.in, got, c.want)
		}
	}
}
