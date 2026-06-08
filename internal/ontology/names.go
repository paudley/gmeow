// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package ontology

import "strings"

// The gmeow names vocabulary (canonical: ~/Active/gmeow-ontology — module
// names.ttl, commit 1ed35af; docs/names-mapping.md). GMEOW is the PRIMARY
// ontology: the importer emits a reified, co-equal gmeow:PersonName appellation
// per name (the schema:/foaf:/vcard: forms are aligned via SSSOM/EDOAL). A name is
// NEVER a bare datatype property: there is deliberately no primary/preferred name,
// no imposed given+family split, and display SELECTION is locale-relative — only
// display SUPPRESSION (gmeow:displayable false, deadnames) is modelled.
const (
	// Appellation classes + bearing.
	AppellationClass = Gmeow + "Appellation"
	PersonNameClass  = Gmeow + "PersonName"
	HasName          = Gmeow + "hasName"  // entity -> PersonName (co-equal; non-functional)
	FullName         = Gmeow + "fullName" // authoritative surface form

	// Structured parts. namePartType is an OPEN value vocabulary (the placeType
	// idiom), never a subclass; partOrder is descriptive, never normative.
	NamePartClass = Gmeow + "NamePart"
	HasNamePart   = Gmeow + "hasNamePart"
	NamePartType  = Gmeow + "namePartType"
	PartText      = Gmeow + "partText"
	PartOrder     = Gmeow + "partOrder"

	// namePartType VALUES we emit from contact formats (the published vocab carries
	// 35; these are the ones vCard/CSV/Apple actually distinguish).
	NamePartGiven              = Gmeow + "namePartGiven"
	NamePartSurname            = Gmeow + "namePartSurname"
	NamePartMiddle             = Gmeow + "namePartMiddle"
	NamePartNickname           = Gmeow + "namePartNickname"
	NamePartHonorificPrefix    = Gmeow + "namePartHonorificPrefix"
	NamePartHonorificSuffix    = Gmeow + "namePartHonorificSuffix"
	NamePartGenerationalSuffix = Gmeow + "namePartGenerationalSuffix"

	// Appellation metadata + the ONLY name filter (deadname suppression).
	NameLanguage         = Gmeow + "nameLanguage"
	NameScript           = Gmeow + "nameScript"
	Romanization         = Gmeow + "romanization"
	NamePurpose          = Gmeow + "namePurpose"
	NamePurposeNickname  = Gmeow + "namePurposeNickname"
	Displayable          = Gmeow + "displayable"
	Honorific            = Gmeow + "honorific"
	HonorificPosition    = Gmeow + "honorificPosition"
	HonorificPositionPre = Gmeow + "honorificPositionPrefix"
	HonorificPositionSuf = Gmeow + "honorificPositionSuffix"
)

// honorificForms mirrors the 17 published gmeow:Honorific individuals (names.ttl):
// surface form -> { value IRI, prefix? }. It is the SINGLE source the name
// normalizer reads to strip honorific tokens from a name for comparison and the
// emitter reads to link a recognized title to its gmeow:Honorific value — grounded
// in the ontology rather than an ad-hoc stop list. Keys are normalized (lowercase,
// no trailing dot); "-san"/"-sama" match the bare suffix too.
var honorificForms = map[string]struct {
	IRI    string
	Prefix bool
}{
	"mr":     {Gmeow + "honorificMr", true},
	"mrs":    {Gmeow + "honorificMrs", true},
	"ms":     {Gmeow + "honorificMs", true},
	"mx":     {Gmeow + "honorificMx", true},
	"dr":     {Gmeow + "honorificDr", true},
	"prof":   {Gmeow + "honorificProf", true},
	"rev":    {Gmeow + "honorificRev", true},
	"hon":    {Gmeow + "honorificHon", true},
	"sir":    {Gmeow + "honorificSir", true},
	"dame":   {Gmeow + "honorificDame", true},
	"lord":   {Gmeow + "honorificLord", true},
	"lady":   {Gmeow + "honorificLady", true},
	"sri":    {Gmeow + "honorificSri", true},
	"smt":    {Gmeow + "honorificSmt", true},
	"sayyid": {Gmeow + "honorificSayyid", true},
	"san":    {Gmeow + "honorificSan", false},
	"sama":   {Gmeow + "honorificSama", false},
}

// generationalForms are the generational suffix tokens (gmeow:namePartGenerational*
// values) stripped from a name for comparison — Jr/Sr distinguish father/son but do
// not by themselves split identity at the token level (handled as a part, not a
// comparison token).
var generationalForms = map[string]bool{
	"jr": true, "jnr": true, "sr": true, "snr": true,
	"i": true, "ii": true, "iii": true, "iv": true, "v": true,
}

// HonorificForValue returns the gmeow:Honorific IRI and prefix flag for a title
// surface form (e.g. "Dr.", "-san"), or ok=false. The form is normalized here.
func HonorificForValue(form string) (iri string, prefix, ok bool) {
	key := normHonorificKey(form)
	if h, found := honorificForms[key]; found {
		return h.IRI, h.Prefix, true
	}

	return "", false, false
}

// IsHonorificToken reports whether a single normalized name token is a published
// honorific (so the name normalizer drops it from the comparison token set).
func IsHonorificToken(token string) bool {
	_, found := honorificForms[normHonorificKey(token)]

	return found
}

// IsGenerationalToken reports whether a token is a generational suffix (Jr/III/…).
func IsGenerationalToken(token string) bool {
	return generationalForms[normHonorificKey(token)]
}

func normHonorificKey(s string) string {
	return strings.Trim(strings.ToLower(strings.TrimSpace(s)), ".-")
}

// NameTokenConcept is the synthetic concept of a single name token — the unit the
// idf-subsumption scorer compares. Name-bearing predicates (fullName, the part
// shortcuts, the schema/foaf/vcard aliases) are decomposed into these by the
// extractor; it is the only name concept that reaches resolution.
const NameTokenConcept = "name-token"

// nameConcepts are the grounded concepts the extractor DECOMPOSES into name tokens
// (rather than passing through as a single comparison claim).
var nameConcepts = map[string]bool{
	"name":            true,
	"name-part":       true, // gmeow:partText on a typed gmeow:NamePart (role-free)
	"given-name":      true,
	"family-name":     true,
	"additional-name": true,
	"nickname":        true,
}

// IsNameConcept reports whether a grounded concept is a name-bearing one the
// extractor should tokenize into NameTokenConcept claims.
func IsNameConcept(concept string) bool { return nameConcepts[concept] }

// NameTokens decomposes a name value into the normalized, role-free comparison
// tokens the idf-subsumption scorer compares: lower-cased, punctuation-stripped,
// with published honorific and generational tokens removed (they are facets, not
// identity). A single-letter initial is kept as a 1-char token (the scorer treats
// it as a loose match against any token sharing its first letter). Order carries no
// meaning. This is the grounded match-key derivation (the FnO fnNameMatchTokens
// candidate); the corpus-statistical idf weighting stays in the resolver.
//
// Examples: "Mr. Patrick Colm Audley" -> [patrick colm audley];
// "Patrick Audley Jr." -> [patrick audley]; "P. Audley" -> [p audley].
func NameTokens(value string) []string {
	fields := strings.FieldsFunc(strings.ToLower(value), func(r rune) bool {
		// split on anything that is not a letter or a digit (folds ".", ",", "-",
		// whitespace); CJK runes are letters and stay whole.
		return !isNameRune(r)
	})

	out := make([]string, 0, len(fields))
	for _, f := range fields {
		if f == "" || IsHonorificToken(f) || IsGenerationalToken(f) {
			continue
		}
		out = append(out, f)
	}

	return out
}

func isNameRune(r rune) bool {
	switch {
	case r >= 'a' && r <= 'z', r >= '0' && r <= '9':
		return true
	case r >= 0x00C0: // keep accented Latin, CJK, and other letters whole
		return true
	default:
		return false
	}
}
