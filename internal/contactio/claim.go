// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"strconv"
	"strings"
)

// Claim is a single contact-domain RDF statement plus optional RDF-star
// annotations (provenance, temporal, confidence). It is the importer-side
// intermediate representation that every format/API mapper targets; the shared
// BuildRDFStarDelta serializer turns []Claim into Turtle so no mapper hand-rolls
// its own string building.
type Claim struct {
	Subject     string
	Predicate   string
	Object      objectTerm
	Annotations []annotation
}

// annotation is an RDF-star statement-level claim hung off << S P O >>.
type annotation struct {
	Predicate string
	Object    objectTerm
}

// objectKind classifies how an object term renders in Turtle. The rendering of
// each kind is byte-identical to the legacy writeTriple/iri/literal helpers so
// migrating a parser onto the Claim model preserves output.
type objectKind int

const (
	objectIRI          objectKind = iota // <value>
	objectLiteral                        // "value"
	objectTypedInteger                   // "value"^^<xsd:integer>
	objectTypedDate                      // "value"^^<xsd:date>
	objectRaw                            // value already rendered (escape hatch)
)

// objectTerm is a rendered-on-demand RDF object value.
type objectTerm struct {
	value string
	kind  objectKind
}

func termIRI(
	value string,
) objectTerm {
	return objectTerm{value: value, kind: objectIRI}
}

func termLiteral(
	value string,
) objectTerm {
	return objectTerm{value: value, kind: objectLiteral}
}

func termTypedDate(value string) objectTerm {
	return objectTerm{value: value, kind: objectTypedDate}
}

func termTypedInteger(value int) objectTerm {
	return objectTerm{value: strconv.Itoa(value), kind: objectTypedInteger}
}

// termRaw wraps an already-rendered Turtle object term. It is the migration
// escape hatch for call sites that still compute their object string with the
// legacy helpers (e.g. objectForFact); such terms are emitted verbatim.
func termRaw(value string) objectTerm {
	return objectTerm{value: value, kind: objectRaw}
}

// render returns the Turtle text for the object term, byte-identical to the
// legacy helpers it replaces.
func (t objectTerm) render() string {
	switch t.kind {
	case objectIRI:
		return iri(t.value)
	case objectLiteral:
		return literal(t.value)
	case objectTypedInteger:
		return literal(t.value) + "^^" + iri(xsdInteger)
	case objectTypedDate:
		return typedDate(t.value)
	case objectRaw:
		return t.value
	default:
		return literal(t.value)
	}
}

// withAnnotation returns the claim with an RDF-star annotation appended.
func (c Claim) withAnnotation(predicate string, object objectTerm) Claim {
	c.Annotations = append(c.Annotations, annotation{Predicate: predicate, Object: object})

	return c
}

// BuildRDFStarDelta renders claims as Turtle triple lines (no @prefix header,
// matching how the legacy writeTriple helper appended to a builder). The base
// triple is always emitted plain; an RDF-star annotation block is emitted only
// when a claim carries annotations. Claim order is preserved as given.
func BuildRDFStarDelta(claims []Claim) string {
	var builder strings.Builder
	for _, claim := range claims {
		writeClaim(&builder, claim)
	}

	return builder.String()
}

func writeClaim(builder *strings.Builder, claim Claim) {
	object := claim.Object.render()

	builder.WriteString(iri(claim.Subject))
	builder.WriteString(" ")
	builder.WriteString(iri(claim.Predicate))
	builder.WriteString(" ")
	builder.WriteString(object)
	builder.WriteString(" .\n")

	if len(claim.Annotations) == 0 {
		return
	}

	statement := "<< " + iri(
		claim.Subject,
	) + " " + iri(
		claim.Predicate,
	) + " " + object + " >> "
	for _, ann := range claim.Annotations {
		builder.WriteString(statement)
		builder.WriteString(iri(ann.Predicate))
		builder.WriteString(" ")
		builder.WriteString(ann.Object.render())
		builder.WriteString(" .\n")
	}
}
