// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

// The canonical term set — standards-first. Each entry is the ONE canonical
// predicate for a contact concept, with the resolution semantics every consumer
// reads. Source vocabularies (vCard EMAIL, apple Email, csv "E-mail Address", …)
// map TO these via the per-format source→term tables (sources.go); nothing else
// is a comparison predicate. Only concept-bearing terms live here; structural
// scaffolding (schema:contactPoint, schema:about, rdf:type, contactType) is not a
// comparison claim and is dropped by the extractor (ByIRI returns false).
//
// Adding/retiring a term here changes behavior everywhere at once — which is the
// point: one source of truth. `gmeow:` terms carry a GmeowReason and must be
// registered in the published ontology (docs/ontology/gmeow-terms.md).

func buildRegistry() map[string]Term {
	terms := []Term{
		// --- Names (functional spine + contextual parts) ---
		{
			IRI:        Schema + "name",
			Concept:    "name",
			Kind:       Functional,
			Role:       RoleNone,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "givenName",
			Concept:    "given-name",
			Kind:       Contextual,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "familyName",
			Concept:    "family-name",
			Kind:       Contextual,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "additionalName",
			Concept:    "additional-name",
			Kind:       Contextual,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "nick",
			Concept:    "nickname",
			Kind:       Set,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "alternateName",
			Concept:    "nickname",
			Kind:       Set,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},

		// --- Locators (set; value-bearing object on the ContactPoint node) ---
		{
			IRI:        Schema + "email",
			Concept:    "email",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjEmail,
			Normalize:  NormEmail,
		},
		{
			IRI:        Schema + "telephone",
			Concept:    "phone",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjTel,
			Normalize:  NormPhone,
		},
		{
			IRI:        Schema + "url",
			Concept:    "url",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},

		// --- Standard-vocabulary variants of the above concepts (rooted RDF and
		// other importers may emit any of these; all converge to one concept). ---
		{
			IRI:        FOAF + "name",
			Concept:    "name",
			Kind:       Functional,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        VCard + "fn",
			Concept:    "name",
			Kind:       Functional,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        VCard + "hasName",
			Concept:    "name",
			Kind:       Functional,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        VCard + "nickname",
			Concept:    "nickname",
			Kind:       Set,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "givenName",
			Concept:    "given-name",
			Kind:       Contextual,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "familyName",
			Concept:    "family-name",
			Kind:       Contextual,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "mbox",
			Concept:    "email",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjEmail,
			Normalize:  NormEmail,
		},
		{
			IRI:        VCard + "hasEmail",
			Concept:    "email",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjEmail,
			Normalize:  NormEmail,
		},
		{
			IRI:        VCard + "email",
			Concept:    "email",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjEmail,
			Normalize:  NormEmail,
		},
		{
			IRI:        VCard + "hasTelephone",
			Concept:    "phone",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjTel,
			Normalize:  NormPhone,
		},
		{
			IRI:        VCard + "tel",
			Concept:    "phone",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjTel,
			Normalize:  NormPhone,
		},
		{
			IRI:        FOAF + "phone",
			Concept:    "phone",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjTel,
			Normalize:  NormPhone,
		},
		{
			IRI:        FOAF + "homepage",
			Concept:    "url",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},
		{
			IRI:        VCard + "hasURL",
			Concept:    "url",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},
		{
			IRI:        VCard + "url",
			Concept:    "url",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},

		// --- Online / IM / social accounts: the account NODE IRI is the comparison
		// value (it encodes service+handle deterministically). accountName/
		// accountServiceHomepage are scaffolding on the node, dropped by the
		// extractor, not comparison terms. ---
		{
			IRI:        FOAF + "account",
			Concept:    "account",
			Kind:       Set,
			Role:       RoleAccount,
			ObjectKind: ObjNode,
			Normalize:  NormURL,
		},

		// --- Postal address parts (contextual; the PostalAddress node groups them) ---
		{
			IRI:        Schema + "streetAddress",
			Concept:    "street-address",
			Kind:       Contextual,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "addressLocality",
			Concept:    "locality",
			Kind:       Contextual,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "addressRegion",
			Concept:    "region",
			Kind:       Contextual,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "postalCode",
			Concept:    "postal-code",
			Kind:       Set,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "addressCountry",
			Concept:    "country",
			Kind:       Contextual,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},

		// --- Organization / employment ---
		{
			IRI:        Schema + "worksFor",
			Concept:    "works-for",
			Kind:       Set,
			Role:       RoleOrgRole,
			ObjectKind: ObjNode,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "jobTitle",
			Concept:    "job-title",
			Kind:       Contextual,
			Role:       RoleOrgRole,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "department",
			Concept:    "department",
			Kind:       Contextual,
			Role:       RoleOrgRole,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Org + "memberOf",
			Concept:    "member-of",
			Kind:       Set,
			Role:       RoleOrgRole,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},

		// --- Personal facts ---
		{
			IRI:        Schema + "birthDate",
			Concept:    "birth-date",
			Kind:       Functional,
			ObjectKind: ObjDate,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "gender",
			Concept:    "gender",
			Kind:       Functional,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "image",
			Concept:    "image",
			Kind:       Set,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},
		{
			IRI:        Schema + "description",
			Concept:    "note",
			Kind:       Contextual,
			Role:       RoleNote,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        DCTerms + "description",
			Concept:    "note",
			Kind:       Contextual,
			Role:       RoleNote,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},

		// --- Relationships ---
		{
			IRI:        Schema + "knows",
			Concept:    "knows",
			Kind:       Set,
			Role:       RoleRelationship,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},
		{
			IRI:        Schema + "colleague",
			Concept:    "colleague",
			Kind:       Set,
			Role:       RoleRelationship,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},

		// --- External identity links ---
		{
			IRI:        OWL + "sameAs",
			Concept:    "same-as",
			Kind:       Set,
			Role:       RoleIdentifier,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},
		{
			IRI:        Schema + "sameAs",
			Concept:    "same-as",
			Kind:       Set,
			Role:       RoleIdentifier,
			ObjectKind: ObjIRI,
			Normalize:  NormURL,
		},

		// --- Local gmeow: relationship terms (no clean standard; registered) ---
		{
			IRI:         Gmeow + "hasMet",
			Concept:     "has-met",
			Kind:        Set,
			Role:        RoleRelationship,
			ObjectKind:  ObjIRI,
			Normalize:   NormURL,
			GmeowReason: "no standard temporally-scoped has-met relationship",
		},
		{
			IRI:         Gmeow + "hasWorkedWith",
			Concept:     "worked-with",
			Kind:        Set,
			Role:        RoleRelationship,
			ObjectKind:  ObjIRI,
			Normalize:   NormURL,
			GmeowReason: "rel: lacks a worked-with distinction",
		},
		{
			IRI:         Gmeow + "hasAgreement",
			Concept:     "has-agreement",
			Kind:        Set,
			Role:        RoleRelationship,
			ObjectKind:  ObjIRI,
			Normalize:   NormURL,
			GmeowReason: "no standard agreement relationship",
		},
	}

	out := make(map[string]Term, len(terms))
	for _, t := range terms {
		out[t.IRI] = t
	}

	return out
}
