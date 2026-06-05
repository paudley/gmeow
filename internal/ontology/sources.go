// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

import "strings"

// Per-format source-vocabulary → canonical-concept maps. These are the ONLY
// place a source property name is interpreted; the importers no longer invent
// per-format predicates. A source property maps to a Mapping describing the
// canonical concept to emit (the emitter turns concept+value into the standards-
// first node structure) plus, for accounts, the service it denotes.
//
// Properties NOT in a map are either genuine source-metadata / vendor extensions
// (kept as provenance via the importer's existing long-tail path, not comparison
// claims) or unmapped (rejected per the completeness contract).

// Mapping is the canonical disposition of one source property.
type Mapping struct {
	Concept string // canonical concept ("email","phone","name","account",…)
	Service string // for Concept=="account": the service homepage IRI
}

// ConceptTerm resolves a canonical concept to its Term (the comparison terms are
// keyed by IRI in the registry; this indexes them by concept for the emitter).
func ConceptTerm(concept string) (Term, bool) {
	t, ok := conceptIndex[concept]

	return t, ok
}

// KindForConcept returns the idDiff Kind for a canonical concept, defaulting to
// Contextual for unknown concepts (the safe default). This is the single
// concept→kind authority the resolution engine reads.
func KindForConcept(concept string) Kind {
	// name-token is a SYNTHETIC concept (no predicate): the extractor mints it from
	// the name-bearing predicates, so it is not in conceptIndex but must score as a
	// name token under idf-subsumption.
	if concept == NameTokenConcept {
		return Name
	}
	if t, ok := conceptIndex[concept]; ok {
		return t.Kind
	}

	return Contextual
}

var conceptIndex = func() map[string]Term {
	out := map[string]Term{}
	// orderedTerms defines the canonical predicate before its aliases, so
	// first-in-order wins → the canonical IRI per concept (schema:name, not vcard:fn).
	for _, t := range orderedTerms() {
		if _, dup := out[t.Concept]; !dup {
			out[t.Concept] = t
		}
	}

	return out
}()

// imAccountServices maps an IM/social source property (and common value
// prefixes) to the account's service homepage IRI — the one OnlineAccount
// pattern replacing the per-service gmeow:*Identity predicates.
var imAccountServices = map[string]string{
	"aim":          "https://www.aim.com/",
	"icq":          "https://icq.com/",
	"jabber":       "xmpp:",
	"xmpp":         "xmpp:",
	"msn":          "https://www.msn.com/",
	"windows-live": "https://www.msn.com/",
	"yahoo":        "https://www.yahoo.com/",
	"skype":        "https://www.skype.com/",
	"google-talk":  "https://talk.google.com/",
	"gtalk":        "https://talk.google.com/",
	"myspace":      "https://myspace.com/",
}

// VCardMapping returns the canonical mapping for a vCard property name (already
// upper-cased by the parser), or ok=false for properties that are not comparison
// concepts (handled as provenance/source-metadata by the importer).
func VCardMapping(prop string) (Mapping, bool) {
	switch prop {
	case "FN":
		return Mapping{Concept: "name"}, true
	case "N":
		return Mapping{Concept: "name-structured"}, true
	case "NICKNAME":
		return Mapping{Concept: "nickname"}, true
	case "X-MAIDENNAME", "X-PHONETIC-LAST-NAME":
		return Mapping{Concept: "family-name"}, true
	case "EMAIL":
		return Mapping{Concept: "email"}, true
	case "TEL", "X-PRIMARY-PHONE":
		return Mapping{Concept: "phone"}, true
	case "URL", "FBURL":
		return Mapping{Concept: "url"}, true
	case "ORG":
		return Mapping{Concept: "works-for"}, true
	case "TITLE", "ROLE":
		return Mapping{Concept: "job-title"}, true
	case "ADR", "ADDRESS":
		return Mapping{Concept: "address"}, true
	case "GEO":
		return Mapping{Concept: "coordinates"}, true
	case "TZ", "TZID":
		return Mapping{Concept: "timezone"}, true
	case "BDAY", "BIRTHDAY":
		return Mapping{Concept: "birth-date"}, true
	case "X-GENDER":
		return Mapping{Concept: "gender"}, true
	case "PHOTO", "LOGO":
		return Mapping{Concept: "image"}, true
	case "NOTE":
		return Mapping{Concept: "note"}, true
	case "AIM", "X-AIM", "X-AIM-ID":
		return Mapping{Concept: "account", Service: imAccountServices["aim"]}, true
	case "ICQ", "X-ICQ", "X-ICQ-ID":
		return Mapping{Concept: "account", Service: imAccountServices["icq"]}, true
	case "JABBER", "X-JABBER", "X-XMPP":
		return Mapping{Concept: "account", Service: imAccountServices["xmpp"]}, true
	case "MSN", "X-MSN", "X-MSN-ID", "X-WINDOWS-LIVE":
		return Mapping{Concept: "account", Service: imAccountServices["msn"]}, true
	case "YAHOO", "X-YAHOO", "X-YAHOO-ID":
		return Mapping{Concept: "account", Service: imAccountServices["yahoo"]}, true
	case "X-SKYPE", "X-SKYPE-ID":
		return Mapping{Concept: "account", Service: imAccountServices["skype"]}, true
	case "GTALK", "GOOGLE-TALK", "X-GTALK":
		return Mapping{Concept: "account", Service: imAccountServices["gtalk"]}, true
	case "X-MYSPACE":
		return Mapping{Concept: "account", Service: imAccountServices["myspace"]}, true
	case "IMPP", "IM", "X-IM", "X-SOCIALPROFILE":
		return Mapping{Concept: "account"}, true // service parsed from the value
	default:
		return Mapping{}, false
	}
}

// serviceShutdown records the date a service endpoint ceased operating, keyed by
// the account's service-homepage IRI. An account on a dead service gets a
// time:hasEnd validity bound (the provider lifecycle), per AGENTS.md — bounded to
// the affected endpoint, not the person.
var serviceShutdown = map[string]string{
	"https://www.aim.com/":     "2017-12-15",
	"https://icq.com/":         "2024-06-26",
	"https://www.msn.com/":     "2013-04-30",
	"https://www.yahoo.com/":   "2018-07-17",
	"https://talk.google.com/": "2017-06-26",
}

// ServiceShutdown returns the endpoint-shutdown date for an account service, if
// known.
func ServiceShutdown(service string) (string, bool) {
	date, ok := serviceShutdown[service]

	return date, ok
}

// ServiceForIMValue extracts the service homepage from an IM value like
// "skype:handle" or "xmpp:user@host" (used when the property itself doesn't name
// the service, e.g. IMPP/IM).
func ServiceForIMValue(value string) string {
	if scheme, _, found := strings.Cut(value, ":"); found {
		if svc, ok := imAccountServices[strings.ToLower(scheme)]; ok {
			return svc
		}
	}

	return ""
}
