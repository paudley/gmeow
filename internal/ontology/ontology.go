// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

// Package ontology is the single, authoritative, code-side encoding of the
// gmeow contact ontology that docs/ontology/ documents. It is standards-first:
// every contact concept has ONE canonical predicate drawn from an established
// vocabulary (schema.org, FOAF, vCard-RDF, ORG, REL, PROV, TIME, GEDCOM,
// SKOS/OWL), and a `gmeow:` term is used only where no standard expresses the
// concept (each carries a GmeowReason and must be registered in the published
// gmeow: ontology).
//
// It is the grounding for normalization: any emitted predicate IRI resolves via
// ByIRI to its canonical Term, which carries the resolution semantics the
// importers, the idDiff engine, and the QUERY projection all need — the
// comparison Concept, the idDiff Kind (functional/set/contextual), the entity
// Role, the temporal Role, the object kind, and the per-concept value
// normalization. There is exactly one such mapping; nothing normalizes ad-hoc.
//
// This package is a leaf: it imports only the standard library, so contactio,
// embedding, and the projection can all depend on it with no import cycle.
package ontology

import "strings"

// Namespaces — standards-first; gmeow: only for genuinely-local concepts.
const (
	Schema  = "https://schema.org/"
	FOAF    = "http://xmlns.com/foaf/0.1/"
	VCard   = "http://www.w3.org/2006/vcard/ns#"
	Org     = "http://www.w3.org/ns/org#"
	Rel     = "http://purl.org/vocab/relationship/"
	Prov    = "http://www.w3.org/ns/prov#"
	DCTerms = "http://purl.org/dc/terms/"
	Time    = "http://www.w3.org/2006/time#"
	GEDCOM  = "http://www.w3.org/2000/10/swap/pim/gedcom#"
	SKOS    = "http://www.w3.org/2004/02/skos/core#"
	OWL     = "http://www.w3.org/2002/07/owl#"
	DOAP    = "http://usefulinc.com/ns/doap#"
	Gmeow   = "https://blackcatinformatics.ca/gmeow/"
)

// Kind is the idDiff resolution semantics of a term (see
// docs/architecture/CONTACT_IDENTITY_RESOLUTION.md §4.1).
type Kind uint8

const (
	// Contextual: free signal (notes, affiliations, name parts) — soft, low ω.
	Contextual Kind = iota
	// Functional: single-valued-at-a-time (full name, gender, birthdate) — a
	// co-valid, unsuperseded mismatch is a contradiction.
	Functional
	// Set: multi-valued identifiers (emails, phones, urls, accounts) — scored by
	// identifying-mass overlap.
	Set
)

// Role is the contact-domain entity role of a term's object (the docs'
// entity_role column).
type Role uint8

const (
	RoleNone Role = iota
	RoleAgentType
	RoleLocator      // email / phone / url / address contact point
	RoleAccount      // online / IM / social account
	RoleRelationship // person↔person edge
	RoleOrgRole      // employment / membership / title
	RoleNote         // free text
	RoleIdentifier   // sameAs / external authority link
	RoleEvent        // dated event
	RoleEvidence     // provenance / annotation
)

// Temporal is which temporal slot a term's value feeds (docs' temporal column).
type Temporal uint8

const (
	TemporalNone        Temporal = iota
	TemporalObservation          // when the source recorded it (prov:generatedAtTime)
	TemporalValidity             // when it was true in the world (time:hasBeginning/End)
)

// ObjectKind is the shape of a term's object.
type ObjectKind uint8

const (
	ObjLiteral ObjectKind = iota
	ObjEmail              // mailto: IRI / address literal
	ObjTel                // tel: IRI / number literal
	ObjIRI                // a resource IRI
	ObjNode               // a structured blank/named node (PostalAddress, OnlineAccount)
	ObjDate               // xsd:date / dateTime
)

// Term is a canonical ontology term plus the resolution semantics every consumer
// needs. Normalize canonicalizes the object value for comparison/keying (the
// persisted record keeps the source value; only the compared/keyed form is
// normalized). GmeowReason is non-empty iff IRI is in the gmeow: namespace.
type Term struct {
	IRI         string
	Concept     string
	Kind        Kind
	Role        Role
	Temporal    Temporal
	ObjectKind  ObjectKind
	Normalize   func(string) string
	GmeowReason string
}

// IsGmeow reports whether the term is a local gmeow: term (must have a reason).
func (t Term) IsGmeow() bool { return strings.HasPrefix(t.IRI, Gmeow) }

// registry maps a canonical predicate IRI to its Term. Built once from terms.go.
var registry = buildRegistry()

// ByIRI resolves an emitted predicate IRI to its canonical Term. This is THE
// grounding used by the normalizer: a predicate the importers emit (always a
// canonical term, post-Stage-B) resolves to its concept/kind/normalizer.
func ByIRI(predicateIRI string) (Term, bool) {
	t, ok := registry[predicateIRI]

	return t, ok
}

// Concepts returns the distinct comparison concepts, for tests/introspection.
func Concepts() []string {
	seen := map[string]bool{}
	out := []string{}

	for _, t := range registry {
		if t.Concept != "" && !seen[t.Concept] {
			seen[t.Concept] = true
			out = append(out, t.Concept)
		}
	}

	return out
}

// Terms returns all canonical terms (stable order not guaranteed), for the
// completeness test against docs/ontology.
func Terms() []Term {
	out := make([]Term, 0, len(registry))
	for _, t := range registry {
		out = append(out, t)
	}

	return out
}
