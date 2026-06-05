// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

import (
	"reflect"
	"testing"
)

func TestNameTokensStripsHonorificsAndGenerational(t *testing.T) {
	cases := []struct {
		in   string
		want []string
	}{
		{"Patrick", []string{"patrick"}},
		{"Patrick Audley", []string{"patrick", "audley"}},
		{"Patrick Colm Audley", []string{"patrick", "colm", "audley"}},
		{"Mr. Patrick Audley", []string{"patrick", "audley"}},    // honorific stripped
		{"Dr Patrick Audley Jr.", []string{"patrick", "audley"}}, // honorific + generational
		{
			"Charles Beaumont III",
			[]string{"charles", "beaumont"},
		}, // roman-numeral generational
		{
			"P. Audley",
			[]string{"p", "audley"},
		}, // initial kept as 1-char token
		{"  Patrick   AUDLEY  ", []string{"patrick", "audley"}}, // whitespace + case fold
		{"欧德理", []string{"欧德理"}},                                // CJK kept whole
	}
	for _, c := range cases {
		got := NameTokens(c.in)
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("NameTokens(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func TestHonorificGrounding(t *testing.T) {
	if iri, prefix, ok := HonorificForValue(
		"Dr.",
	); !ok || iri != Gmeow+"honorificDr" ||
		!prefix {
		t.Fatalf("Dr. -> %q prefix=%v ok=%v", iri, prefix, ok)
	}
	if iri, prefix, ok := HonorificForValue(
		"-san",
	); !ok || iri != Gmeow+"honorificSan" ||
		prefix {
		t.Fatalf("-san -> %q prefix=%v ok=%v", iri, prefix, ok)
	}
	if !IsNameConcept("name") || !IsNameConcept("given-name") || IsNameConcept("email") {
		t.Fatalf("IsNameConcept misclassified")
	}
	if KindForConcept(NameTokenConcept) != Name {
		t.Fatalf("name-token concept must score as KindName")
	}
}
