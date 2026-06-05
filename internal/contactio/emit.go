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
		// A junk EMAIL value (no @) is preserved as evidence — a schema:email
		// literal, not a coerced mailto: node (the doc's typed-fallback rule).
		if norm := ontology.NormEmail(value); strings.Contains(norm, "@") {
			return emitContactPoint(
				claims,
				subject,
				"mailto:",
				norm,
				ontology.Schema+"email",
				sourceProp,
			)
		}

		return appendCanonical(claims, subject, "email", termLiteral(value), sourceProp)
	case "phone":
		if norm := ontology.NormPhone(value); isAllDigits(norm) {
			return emitContactPoint(
				claims,
				subject,
				"tel:",
				norm,
				ontology.Schema+"telephone",
				sourceProp,
			)
		}

		return appendCanonical(claims, subject, "phone", termLiteral(value), sourceProp)
	case "account":
		return emitAccount(claims, subject, mapping.Service, value, sourceProp)
	case "address":
		return emitAddress(claims, subject, value, sourceProp)
	case "coordinates":
		return emitCoordinates(claims, subject, value, sourceProp)
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
		// Provider lifecycle: a dead service bounds the account's validity (the
		// endpoint, not the person).
		if end, ok := ontology.ServiceShutdown(service); ok {
			claims = append(claims, Claim{
				Subject:   node,
				Predicate: ontology.Time + "hasEnd",
				Object:    termTypedDate(end),
			})
		}
	}
	claims = append(
		claims,
		Claim{Subject: node, Predicate: schemaAboutPred, Object: termIRI(subject)},
	)

	return claims
}

func isAllDigits(s string) bool {
	if s == "" {
		return false
	}
	for _, r := range s {
		if r < '0' || r > '9' {
			return false
		}
	}

	return true
}

// emitAddress emits a gmeow:PostalAddress surface node (GMEOW-primary; the gmeow:
// address components are owl:equivalentProperty to schema:). A ";"-delimited vCard
// ADR (pobox;ext;street;locality;region;postal;country) yields all SEVEN gmeow
// components; otherwise the whole value is the street address.
func emitAddress(claims []Claim, subject, value, sourceProp string) []Claim {
	node := "urn:gmeow:addr:" + shortHash([]byte(strings.ToLower(value)))

	claims = append(
		claims,
		Claim{
			Subject:   subject,
			Predicate: ontology.HasContactPointPlaces,
			Object:    termIRI(node),
		},
	)
	claims = append(
		claims,
		Claim{
			Subject:   node,
			Predicate: rdfTypePred,
			Object:    termIRI(ontology.PostalAddressClass),
		},
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
		addPart(ontology.PostOfficeBox, parts[0])
		addPart(ontology.ExtendedAddress, parts[1])
		addPart(ontology.StreetAddress, parts[2])
		addPart(ontology.AddressLocality, parts[3])
		addPart(ontology.AddressRegion, parts[4])
		addPart(ontology.PostalCode, parts[5])
		addPart(ontology.CountryCode, parts[6])
	} else {
		addPart(ontology.StreetAddress, value)
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

// emitCoordinates emits a gmeow:Place (premises granularity) carrying its
// gmeow:latitude/longitude, linked from the agent via gmeow:locatedAt — the
// surface→resolved seam (a later gazetteer step gives the Place a QID and the
// containedInPlace hierarchy). Accepts a vCard GEO value "geo:lat,long"
// (RFC 6350) or legacy "lat;long". (Coordinates are flattened onto the Place
// rather than a nested gmeow:GeoCoordinates node so the place is preserved as a
// comparison-bearing delta node; the nested form is a later refinement.)
func emitCoordinates(claims []Claim, subject, value, sourceProp string) []Claim {
	lat, long, ok := parseGeo(value)
	if !ok {
		return claims
	}

	place := "urn:gmeow:place:" + shortHash([]byte(lat+","+long))

	claims = append(
		claims,
		Claim{Subject: subject, Predicate: ontology.LocatedAt, Object: termIRI(place)},
	)
	claims = append(
		claims,
		Claim{Subject: place, Predicate: rdfTypePred, Object: termIRI(ontology.PlaceClass)},
	)
	claims = append(
		claims,
		Claim{
			Subject:   place,
			Predicate: ontology.PlaceType,
			Object:    termIRI(ontology.PlaceTypePremises),
		},
	)
	claims = append(
		claims,
		Claim{Subject: place, Predicate: ontology.Latitude, Object: termLiteral(lat)},
	)
	claims = append(
		claims,
		Claim{Subject: place, Predicate: ontology.Longitude, Object: termLiteral(long)},
	)

	if sourceProp != "" {
		claims = append(
			claims,
			Claim{
				Subject:   place,
				Predicate: gmeowMappedFromPred,
				Object:    termLiteral(sourceProp),
			},
		)
	}

	return claims
}

// parseGeo extracts decimal latitude/longitude from a vCard GEO value:
// "geo:53.54,-113.92" (RFC 6350 URI) or the legacy "53.54;-113.92".
func parseGeo(value string) (lat, long string, ok bool) {
	v := strings.TrimSpace(value)
	v = strings.TrimPrefix(strings.TrimPrefix(v, "geo:"), "GEO:")

	sep := ","
	if !strings.Contains(v, ",") && strings.Contains(v, ";") {
		sep = ";"
	}

	parts := strings.SplitN(v, sep, 2)
	if len(parts) != 2 {
		return "", "", false
	}

	lat = strings.TrimSpace(parts[0])
	long = strings.TrimSpace(parts[1])
	if !isDecimal(lat) || !isDecimal(long) {
		return "", "", false
	}

	return lat, long, true
}

// isDecimal reports whether s is a signed decimal number (a coordinate degree).
func isDecimal(s string) bool {
	if s == "" {
		return false
	}
	seenDigit, seenDot := false, false
	for i, r := range s {
		switch {
		case r >= '0' && r <= '9':
			seenDigit = true
		case (r == '-' || r == '+') && i == 0:
		case r == '.' && !seenDot:
			seenDot = true
		default:
			return false
		}
	}

	return seenDigit
}
