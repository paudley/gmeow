// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"strings"

	"blackcat.ca/gmeow/internal/ontology"
)

// Canonical scaffolding predicates/types (standards-first node structures).
const (
	schemaContactPointType = ontology.Schema + "ContactPoint"
	schemaContactPointPred = ontology.Schema + "contactPoint"
	schemaAboutPred        = ontology.Schema + "about"
	schemaPostalAddrType   = ontology.Schema + "PostalAddress"
	schemaAddressPred      = ontology.Schema + "address"
	foafOnlineAccountType  = ontology.FOAF + "OnlineAccount"
	foafAccountPred        = ontology.FOAF + "account"
	foafAccountNamePred    = ontology.FOAF + "accountName"
	foafAccountServicePred = ontology.FOAF + "accountServiceHomepage"
	rdfTypePred            = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	gmeowMappedFromPred    = ontology.Gmeow + "mappedFrom"
)

// emitCanonical appends the standards-first, node-structured claims for one
// source-property value to claims, driven by the ontology mapping. This is the
// single emitter every importer uses: a locator becomes a schema:ContactPoint
// node keyed by its mailto:/tel: IRI; an account becomes a foaf:OnlineAccount
// node; an address becomes a schema:PostalAddress node; literal concepts attach
// directly. Source provenance is preserved as a gmeow:mappedFrom RDF* annotation.
func emitCanonical(
	claims []Claim,
	subject string,
	mapping ontology.Mapping,
	value, sourceProp string,
) []Claim {
	value = strings.TrimSpace(value)
	if value == "" {
		return claims
	}

	switch mapping.Concept {
	case "email":
		return emitContactPoint(claims, subject, "mailto:", ontology.NormEmail(value),
			ontology.Schema+"email", sourceProp)
	case "phone":
		return emitContactPoint(claims, subject, "tel:", ontology.NormPhone(value),
			ontology.Schema+"telephone", sourceProp)
	case "account":
		return emitAccount(claims, subject, mapping.Service, value, sourceProp)
	case "address":
		return emitAddress(claims, subject, value, sourceProp)
	case "url", "image":
		return appendCanonical(
			claims,
			subject,
			mapping.Concept,
			termIRI(ontology.NormURL(value)),
			sourceProp,
		)
	default:
		// Literal concepts (name, nickname, family-name, job-title, works-for,
		// birth-date, gender, note, …) attach directly to the subject.
		return appendCanonical(
			claims,
			subject,
			mapping.Concept,
			termLiteral(value),
			sourceProp,
		)
	}
}

// appendCanonical attaches a single canonical claim (subject → term → object)
// with a mappedFrom provenance annotation, looking the predicate up by concept.
func appendCanonical(
	claims []Claim,
	subject, concept string,
	object objectTerm,
	sourceProp string,
) []Claim {
	term, ok := ontology.ConceptTerm(concept)
	if !ok {
		return claims
	}

	claim := Claim{Subject: subject, Predicate: term.IRI, Object: object}
	if sourceProp != "" {
		claim = claim.withAnnotation(gmeowMappedFromPred, termLiteral(sourceProp))
	}

	return append(claims, claim)
}

// emitContactPoint emits a schema:ContactPoint node keyed by its canonical
// scheme IRI (mailto:/tel:), linked to the agent via schema:about and from the
// agent via schema:contactPoint.
func emitContactPoint(
	claims []Claim,
	subject, scheme, normValue, valuePred, sourceProp string,
) []Claim {
	if normValue == "" {
		return claims
	}

	node := scheme + normValue

	claims = append(
		claims,
		Claim{Subject: subject, Predicate: schemaContactPointPred, Object: termIRI(node)},
	)
	claims = append(
		claims,
		Claim{Subject: node, Predicate: rdfTypePred, Object: termIRI(schemaContactPointType)},
	)
	valueClaim := Claim{Subject: node, Predicate: valuePred, Object: termIRI(node)}
	if sourceProp != "" {
		valueClaim = valueClaim.withAnnotation(gmeowMappedFromPred, termLiteral(sourceProp))
	}
	claims = append(claims, valueClaim)
	claims = append(
		claims,
		Claim{Subject: node, Predicate: schemaAboutPred, Object: termIRI(subject)},
	)

	return claims
}

// emitAccount emits a foaf:OnlineAccount node — the single pattern for every
// service. The account IRI is the profile URL when the value is one, else a
// stable urn synthesized from service + handle.
func emitAccount(claims []Claim, subject, service, value, sourceProp string) []Claim {
	handle := value
	if svc := ontology.ServiceForIMValue(value); service == "" && svc != "" {
		service = svc
		if _, rest, found := strings.Cut(value, ":"); found {
			handle = rest
		}
	}
	handle = strings.TrimSpace(handle)
	if handle == "" {
		return claims
	}

	var node string
	if strings.HasPrefix(strings.ToLower(handle), "http://") ||
		strings.HasPrefix(strings.ToLower(handle), "https://") {
		node = ontology.NormURL(handle)
	} else {
		node = "urn:gmeow:account:" + shortHash(
			[]byte(service+"\x00"+strings.ToLower(handle)),
		)
	}

	claims = append(
		claims,
		Claim{Subject: subject, Predicate: foafAccountPred, Object: termIRI(node)},
	)
	claims = append(
		claims,
		Claim{Subject: node, Predicate: rdfTypePred, Object: termIRI(foafOnlineAccountType)},
	)
	nameClaim := Claim{
		Subject:   node,
		Predicate: foafAccountNamePred,
		Object:    termLiteral(handle),
	}
	if sourceProp != "" {
		nameClaim = nameClaim.withAnnotation(gmeowMappedFromPred, termLiteral(sourceProp))
	}
	claims = append(claims, nameClaim)
	if service != "" {
		claims = append(
			claims,
			Claim{Subject: node, Predicate: foafAccountServicePred, Object: termIRI(service)},
		)
	}
	claims = append(
		claims,
		Claim{Subject: node, Predicate: schemaAboutPred, Object: termIRI(subject)},
	)

	return claims
}

// emitAddress emits a schema:PostalAddress node. A ";"-delimited value (vCard
// ADR: pobox;ext;street;locality;region;postal;country) is split into parts;
// otherwise the whole value is the street address.
func emitAddress(claims []Claim, subject, value, sourceProp string) []Claim {
	node := "urn:gmeow:addr:" + shortHash([]byte(strings.ToLower(value)))

	claims = append(
		claims,
		Claim{Subject: subject, Predicate: schemaAddressPred, Object: termIRI(node)},
	)
	claims = append(
		claims,
		Claim{Subject: node, Predicate: rdfTypePred, Object: termIRI(schemaPostalAddrType)},
	)

	addPart := func(pred, part string) {
		part = strings.TrimSpace(part)
		if part != "" {
			claims = append(
				claims,
				Claim{Subject: node, Predicate: pred, Object: termLiteral(part)},
			)
		}
	}

	if parts := strings.Split(value, ";"); len(parts) >= 7 {
		addPart(ontology.Schema+"streetAddress", parts[2])
		addPart(ontology.Schema+"addressLocality", parts[3])
		addPart(ontology.Schema+"addressRegion", parts[4])
		addPart(ontology.Schema+"postalCode", parts[5])
		addPart(ontology.Schema+"addressCountry", parts[6])
	} else {
		addPart(ontology.Schema+"streetAddress", value)
	}

	if sourceProp != "" {
		claims = append(
			claims,
			Claim{
				Subject:   node,
				Predicate: gmeowMappedFromPred,
				Object:    termLiteral(sourceProp),
			},
		)
	}

	return claims
}
