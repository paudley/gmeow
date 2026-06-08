// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"strings"
	"testing"
)

// legacyTriple renders a single triple the way the pre-Claim writeTriple helper
// did, so the serializer can be asserted byte-identical to the code it replaces.
func legacyTriple(subject, predicate, object string) string {
	var builder strings.Builder
	writeTriple(&builder, subject, predicate, object)

	return builder.String()
}

func TestBuildRDFStarDeltaMatchesLegacyTriple(t *testing.T) {
	subject := "https://example.test/#a"
	cases := []struct {
		name  string
		claim Claim
		want  string
	}{
		{
			"iri",
			Claim{Subject: subject, Predicate: rdfType, Object: termIRI(foafPrefix + "Person")},
			legacyTriple(subject, rdfType, iri(foafPrefix+"Person")),
		},
		{
			"literal",
			Claim{
				Subject:   subject,
				Predicate: foafPrefix + "name",
				Object:    termLiteral("Ann Example"),
			},
			legacyTriple(subject, foafPrefix+"name", literal("Ann Example")),
		},
		{
			"typedInteger",
			Claim{
				Subject:   subject,
				Predicate: GmeowImportanceLevel,
				Object:    termTypedInteger(7),
			},
			legacyTriple(subject, GmeowImportanceLevel, typedInteger(7)),
		},
		{
			"typedDate",
			Claim{
				Subject:   subject,
				Predicate: timePrefix + "hasEnd",
				Object:    termTypedDate("2012-01-02"),
			},
			legacyTriple(subject, timePrefix+"hasEnd", typedDate("2012-01-02")),
		},
		{
			"raw",
			Claim{
				Subject:   subject,
				Predicate: foafPrefix + "name",
				Object:    termRaw(literal("X")),
			},
			legacyTriple(subject, foafPrefix+"name", literal("X")),
		},
	}

	for _, testCase := range cases {
		t.Run(testCase.name, func(t *testing.T) {
			if got := BuildRDFStarDelta([]Claim{testCase.claim}); got != testCase.want {
				t.Fatalf("got %q want %q", got, testCase.want)
			}
		})
	}
}

func TestBuildRDFStarDeltaEmitsAnnotationsAfterBaseTriple(t *testing.T) {
	subject := "https://example.test/#a"
	predicate := schemaPrefix + "email"
	claim := Claim{
		Subject:   subject,
		Predicate: predicate,
		Object:    termLiteral("a@b.test"),
	}.
		withAnnotation(
			timePrefix+"hasEnd",
			termTypedDate("2010-01-01"),
		)

	statement := "<< " + iri(
		subject,
	) + " " + iri(
		predicate,
	) + " " + literal(
		"a@b.test",
	) + " >> "
	want := legacyTriple(subject, predicate, literal("a@b.test")) +
		statement + iri(timePrefix+"hasEnd") + " " + typedDate("2010-01-01") + " .\n"

	if got := BuildRDFStarDelta([]Claim{claim}); got != want {
		t.Fatalf("got %q want %q", got, want)
	}
}

func TestBuildRDFStarDeltaPreservesClaimOrder(t *testing.T) {
	claims := []Claim{
		{Subject: "s1", Predicate: "p", Object: termLiteral("first")},
		{Subject: "s2", Predicate: "p", Object: termLiteral("second")},
	}

	got := BuildRDFStarDelta(claims)
	first := strings.Index(got, "first")
	second := strings.Index(got, "second")
	if first < 0 || second < 0 || first > second {
		t.Fatalf("claim order not preserved: %q", got)
	}
}
