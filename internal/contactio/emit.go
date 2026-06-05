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
	case "name":
		return emitName(claims, subject, nameInput{Full: value, Source: sourceProp})
	case "name-structured":
		return emitName(claims, subject, parseStructuredName(value, sourceProp))
	case "nickname":
		return emitName(
			claims,
			subject,
			nameInput{Nicknames: []string{value}, Source: sourceProp},
		)
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
// nameInput is the assembled name of one record: a surface form and/or structured
// parts plus nicknames, with the source property for provenance. Honorific and
// generational affixes are facets (kept reified for interop, never identity tokens).
type nameInput struct {
	Full      string
	Given     string
	Family    string
	Middle    string
	Prefix    string // honorific prefix (Dr, Mr, …)
	Suffix    string // honorific/generational suffix (Jr, PhD, …)
	Nicknames []string
	Source    string
}

// emitName emits a reified gmeow:PersonName appellation (the names model forbids a
// bare name datatype property): a node keyed per record, linked via gmeow:hasName,
// carrying gmeow:fullName (the surface form, composed from parts when none is given)
// and the structured components as TYPED gmeow:NamePart nodes — the only canonical
// home for a name component (the flat givenNamePart/surnamePart shortcuts were
// retired from the ontology; components are always hasNamePart + namePartType +
// partText). The partText grounds to a role-free name token for comparison; a
// recognized honorific also links its gmeow:Honorific value. The node is keyed by
// the record subject so a card's FN + N + NICKNAME accumulate on ONE co-equal
// appellation.
func emitName(claims []Claim, subject string, n nameInput) []Claim {
	full := strings.TrimSpace(n.Full)
	if full == "" {
		full = composeFullName(n)
	}
	if full == "" && n.Given == "" && n.Family == "" && len(n.Nicknames) == 0 {
		return claims
	}

	node := "urn:gmeow:name:" + shortHash([]byte(strings.ToLower(subject)))
	claims = append(
		claims,
		Claim{Subject: subject, Predicate: ontology.HasName, Object: termIRI(node)},
		Claim{
			Subject:   node,
			Predicate: rdfTypePred,
			Object:    termIRI(ontology.PersonNameClass),
		},
	)
	if full != "" {
		claims = append(
			claims,
			withMappedFrom(
				Claim{Subject: node, Predicate: ontology.FullName, Object: termLiteral(full)},
				n.Source,
			),
		)
	}

	claims = emitNamePart(claims, node, ontology.NamePartGiven, n.Given, n.Source)
	claims = emitNamePart(claims, node, ontology.NamePartSurname, n.Family, n.Source)
	claims = emitNamePart(claims, node, ontology.NamePartMiddle, n.Middle, n.Source)
	for _, nick := range n.Nicknames {
		claims = emitNamePart(claims, node, ontology.NamePartNickname, nick, n.Source)
	}
	claims = emitNamePart(
		claims,
		node,
		ontology.NamePartHonorificPrefix,
		n.Prefix,
		n.Source,
	)
	claims = emitNamePart(
		claims,
		node,
		ontology.NamePartHonorificSuffix,
		n.Suffix,
		n.Source,
	)

	return claims
}

// composeFullName builds a surface form from structured parts (prefix given middle
// family suffix) when a record carries only the parts (Apple/CSV/vCard-N).
func composeFullName(n nameInput) string {
	parts := []string{n.Prefix, n.Given, n.Middle, n.Family, n.Suffix}
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		if p = strings.TrimSpace(p); p != "" {
			out = append(out, p)
		}
	}

	return strings.Join(out, " ")
}

// emitNamePart hangs one reified gmeow:NamePart (hasNamePart + namePartType +
// partText) off the appellation — the canonical form of every name component. The
// partText grounds to a name token (honorific/generational texts self-strip in
// comparison); an honorific affix additionally links its gmeow:Honorific value.
func emitNamePart(claims []Claim, nameNode, partType, value, source string) []Claim {
	value = strings.TrimSpace(value)
	if value == "" {
		return claims
	}

	part := nameNode + "#" + shortHash([]byte(partType+value))
	claims = append(claims,
		Claim{Subject: nameNode, Predicate: ontology.HasNamePart, Object: termIRI(part)},
		Claim{Subject: part, Predicate: rdfTypePred, Object: termIRI(ontology.NamePartClass)},
		Claim{Subject: part, Predicate: ontology.NamePartType, Object: termIRI(partType)},
		withMappedFrom(
			Claim{Subject: part, Predicate: ontology.PartText, Object: termLiteral(value)},
			source,
		),
	)
	if isHonorificPartType(partType) {
		if iri, _, ok := ontology.HonorificForValue(value); ok {
			claims = append(
				claims,
				Claim{Subject: nameNode, Predicate: ontology.Honorific, Object: termIRI(iri)},
			)
		}
	}

	return claims
}

func isHonorificPartType(partType string) bool {
	return partType == ontology.NamePartHonorificPrefix ||
		partType == ontology.NamePartHonorificSuffix
}

// parseStructuredName parses a vCard N value (Family;Given;Additional;Prefixes;
// Suffixes) into a nameInput. Multi-valued components (comma-separated) keep their
// first value for the flat shortcut; the surface form is composed by emitName.
func parseStructuredName(value, sourceProp string) nameInput {
	fields := strings.Split(value, ";")
	get := func(i int) string {
		if i < len(fields) {
			return strings.TrimSpace(strings.ReplaceAll(fields[i], ",", " "))
		}

		return ""
	}

	return nameInput{
		Family: get(0),
		Given:  get(1),
		Middle: get(2),
		Prefix: get(3),
		Suffix: get(4),
		Source: sourceProp,
	}
}

// nameFieldForPredicate classifies a grounded name predicate into the nameInput
// field it fills, so the string-based importers (CSV, Apple) can collect their name
// columns and reify them onto one gmeow:PersonName node instead of attaching bare
// name properties to the contact.
func nameFieldForPredicate(pred string) (string, bool) {
	switch pred {
	case ontology.Schema + "givenName":
		return "given", true
	case ontology.Schema + "familyName":
		return "family", true
	case ontology.Schema + "additionalName":
		return "middle", true
	case ontology.FOAF + "nick", ontology.Schema + "alternateName":
		return "nick", true
	case ontology.Schema + "name", ontology.FullName, ontology.VCard + "fn":
		return "full", true
	case ontology.Schema + "honorificPrefix":
		return "prefix", true
	case ontology.Schema + "honorificSuffix":
		return "suffix", true
	}

	return "", false
}

// assignNameField sets the nameInput field named by nameFieldForPredicate.
func assignNameField(n *nameInput, field, value string) {
	switch field {
	case "given":
		n.Given = value
	case "family":
		n.Family = value
	case "middle":
		n.Middle = value
	case "nick":
		n.Nicknames = append(n.Nicknames, value)
	case "full":
		n.Full = value
	case "prefix":
		n.Prefix = value
	case "suffix":
		n.Suffix = value
	}
}

// writeNameNode is the string-emitter form of emitName for importers that render
// Turtle directly (CSV, Apple): a reified gmeow:PersonName node keyed by the record
// subject, carrying fullName plus the structured components as TYPED gmeow:NamePart
// nodes (the only canonical form — no flat part shortcuts).
func writeNameNode(body *strings.Builder, subject string, n nameInput) {
	full := strings.TrimSpace(n.Full)
	if full == "" {
		full = composeFullName(n)
	}
	if full == "" && n.Given == "" && n.Family == "" && len(n.Nicknames) == 0 {
		return
	}

	node := "urn:gmeow:name:" + shortHash([]byte(strings.ToLower(subject)))
	writeTriple(body, subject, ontology.HasName, iri(node))
	writeTriple(body, node, rdfTypePred, iri(ontology.PersonNameClass))

	if full != "" {
		writeTriple(body, node, ontology.FullName, literal(full))
	}
	writeNamePart(body, node, ontology.NamePartGiven, n.Given)
	writeNamePart(body, node, ontology.NamePartSurname, n.Family)
	writeNamePart(body, node, ontology.NamePartMiddle, n.Middle)
	for _, nick := range n.Nicknames {
		writeNamePart(body, node, ontology.NamePartNickname, nick)
	}
	writeNamePart(body, node, ontology.NamePartHonorificPrefix, n.Prefix)
	writeNamePart(body, node, ontology.NamePartHonorificSuffix, n.Suffix)
}

// writeNamePart is the string-emitter form of emitNamePart.
func writeNamePart(body *strings.Builder, nameNode, partType, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}

	part := nameNode + "#" + shortHash([]byte(partType+value))
	writeTriple(body, nameNode, ontology.HasNamePart, iri(part))
	writeTriple(body, part, rdfTypePred, iri(ontology.NamePartClass))
	writeTriple(body, part, ontology.NamePartType, iri(partType))
	writeTriple(body, part, ontology.PartText, literal(value))
	if isHonorificPartType(partType) {
		if hiri, _, ok := ontology.HonorificForValue(value); ok {
			writeTriple(body, nameNode, ontology.Honorific, iri(hiri))
		}
	}
}

// withMappedFrom adds the gmeow:mappedFrom provenance annotation when a source
// property is known.
func withMappedFrom(claim Claim, sourceProp string) Claim {
	if sourceProp != "" {
		return claim.withAnnotation(gmeowMappedFromPred, termLiteral(sourceProp))
	}

	return claim
}

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
