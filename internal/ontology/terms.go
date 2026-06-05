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

// orderedTerms is the canonical term set in DEFINITION order: the canonical
// predicate for a concept is defined BEFORE its standard-variant aliases, so the
// concept index (sources.go) deterministically picks the canonical IRI (e.g.
// schema:name, not the foaf:name/vcard:fn aliases) for emission.
func orderedTerms() []Term {
	return []Term{
		// --- Names (GMEOW-primary reified appellation; the gmeow: forms are canonical,
		// schema:/foaf:/vcard: are aligned aliases). All name-bearing concepts are
		// DECOMPOSED to role-free name-token comparison claims by the extractor (see
		// IsNameConcept); the Kind here is moot for comparison (name-token -> KindName
		// via KindForConcept) and exists only so ByIRI grounds the predicate. ---
		{
			IRI:         FullName,
			Concept:     "name",
			Kind:        Name,
			Role:        RoleNone,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; reified appellation surface form (skos:closeMatch schema:name/vcard:fn/foaf:name)",
		},
		{
			IRI:        Schema + "name",
			Concept:    "name",
			Kind:       Name,
			Role:       RoleNone,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			// gmeow:partText is the text of a typed gmeow:NamePart — the ONLY canonical
			// home for a name component (the flat givenNamePart/surnamePart shortcuts
			// were retired from the ontology). The part's namePartType (given/surname/…)
			// is a sibling triple and is irrelevant here: comparison is role-free, so
			// every part text tokenizes the same (honorific/generational parts self-strip
			// via ontology.NameTokens).
			IRI:         PartText,
			Concept:     "name-part",
			Kind:        Name,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; reified NamePart text (no flat part shortcut exists)",
		},
		{
			IRI:        Schema + "givenName",
			Concept:    "given-name",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "familyName",
			Concept:    "family-name",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "additionalName",
			Concept:    "additional-name",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "nick",
			Concept:    "nickname",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        Schema + "alternateName",
			Concept:    "nickname",
			Kind:       Name,
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
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        VCard + "fn",
			Concept:    "name",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        VCard + "hasName",
			Concept:    "name",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        VCard + "nickname",
			Concept:    "nickname",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "givenName",
			Concept:    "given-name",
			Kind:       Name,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:        FOAF + "familyName",
			Concept:    "family-name",
			Kind:       Name,
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

		// --- Postal address parts (GMEOW-primary; the gmeow: form is canonical, the
		// schema: form is the aligned alias. Contextual; the PostalAddress node groups
		// them). GMEOW is now the primary ontology. ---
		{
			IRI:         StreetAddress,
			Concept:     "street-address",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; owl:equivalentProperty schema:streetAddress",
		},
		{
			IRI:        Schema + "streetAddress",
			Concept:    "street-address",
			Kind:       Contextual,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},
		{
			IRI:         ExtendedAddress,
			Concept:     "extended-address",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; vCard ADR extended-address (no exact schema term)",
		},
		{
			IRI:         PostOfficeBox,
			Concept:     "po-box",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; vCard ADR post-office-box",
		},
		{
			IRI:         AddressLocality,
			Concept:     "locality",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; owl:equivalentProperty schema:addressLocality",
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
			IRI:         AddressRegion,
			Concept:     "region",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; owl:equivalentProperty schema:addressRegion",
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
			IRI:         PostalCode,
			Concept:     "postal-code",
			Kind:        Set,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; owl:equivalentProperty schema:postalCode",
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
			IRI:         CountryCode,
			Concept:     "country",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; ISO 3166-1 alpha-2 (vs schema:addressCountry name)",
		},
		{
			IRI:        Schema + "addressCountry",
			Concept:    "country",
			Kind:       Contextual,
			Role:       RoleLocator,
			ObjectKind: ObjLiteral,
			Normalize:  NormText,
		},

		// --- Geo coordinates + timezone (gmeow location module) ---
		{
			IRI:         Latitude,
			Concept:     "latitude",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; geo point latitude (skos:closeMatch wgs84:lat)",
		},
		{
			IRI:         Longitude,
			Concept:     "longitude",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; geo point longitude",
		},
		{
			IRI:         Timezone,
			Concept:     "timezone",
			Kind:        Contextual,
			Role:        RoleLocator,
			ObjectKind:  ObjLiteral,
			Normalize:   NormText,
			GmeowReason: "gmeow-primary; IANA timezone (vCard TZ)",
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
			// schema:affiliation — the organization a person is affiliated with;
			// Apple (Organization) and BBDB (company) emit it. Alias of works-for so
			// employer matches contribute to resolution (defined AFTER worksFor so the
			// canonical IRI for the concept stays schema:worksFor).
			IRI:        Schema + "affiliation",
			Concept:    "works-for",
			Kind:       Set,
			Role:       RoleOrgRole,
			ObjectKind: ObjLiteral,
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
}

func buildRegistry() map[string]Term {
	terms := orderedTerms()

	out := make(map[string]Term, len(terms))
	for _, t := range terms {
		out[t.IRI] = t
	}

	return out
}
