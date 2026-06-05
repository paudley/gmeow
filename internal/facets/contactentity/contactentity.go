// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactentity

import (
	"net/mail"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	FactKindAffiliation  = "affiliation"
	FactKindAlias        = "alias"
	FactKindAddress      = "address"
	FactKindCoordinates  = "coordinates"
	FactKindTimezone     = "timezone"
	FactKindAccount      = "account"
	FactKindContactAlias = "contact_alias"
	FactKindEmail        = "email"
	FactKindIdentifier   = "identifier"
	FactKindImportance   = "importance"
	FactKindName         = "name"
	FactKindNote         = "note"
	FactKindPhone        = "phone"
	FactKindRelationship = "relationship"
	FactKindTitle        = "title"
	FactKindURL          = "url"
)

const (
	mailtoPrefix          = "mailto:"
	contactAliasPredicate = "https://blackcatinformatics.ca/gmeow/contactAlias"
	importancePredicate   = "https://blackcatinformatics.ca/gmeow/importanceLevel"
	schemaOrgHTTPPrefix   = "http://schema.org/"
	schemaOrgHTTPSPrefix  = "https://schema.org/"
	schemaOrgOrganization = schemaOrgHTTPSPrefix + "Organization"
	schemaOrgPerson       = schemaOrgHTTPSPrefix + "Person"

	gmeowPrefix = "https://blackcatinformatics.ca/gmeow/"
	// Sub-nodes carry their values as literals (unlike emails, whose value is the
	// object IRI), so the projection flattens these node IRIs onto the contact that
	// links them via gmeow:hasContactPoint / gmeow:locatedAt / gmeow:hasName.
	gmeowAddrNodePrefix  = "urn:gmeow:addr:"
	gmeowPlaceNodePrefix = "urn:gmeow:place:"
	gmeowNameNodePrefix  = "urn:gmeow:name:"
)

type Statement struct {
	SourceDigest  contracts.ObjectDigest
	StatementHash string
	Subject       string
	Predicate     string
	Object        string
	ObjectKind    string
}

type Annotation struct {
	SourceDigest  contracts.ObjectDigest
	StatementHash string
	Predicate     string
	Object        string
}

type Fact struct {
	SourceDigest  contracts.ObjectDigest
	StatementHash string
	ContactID     string
	FactKind      string
	Value         string
	Predicate     string
	ValidFrom     string
	ValidUntil    string
	Historical    bool
}

type MetadataInput struct {
	RootSubject    string
	TargetSubject  string
	Format         string
	SourceKind     string
	ClaimKind      string
	ImportLevel    int
	HasImportLevel bool
	IdentityHints  []string
}

func Facet(input MetadataInput) contracts.Facet {
	return contracts.Facet{
		Kind:     contracts.ContactEntityFacetKind,
		Metadata: Metadata(input),
	}
}

func Metadata(input MetadataInput) map[string]any {
	metadata := map[string]any{}
	putString(metadata, "root_subject", input.RootSubject)
	putString(metadata, "target_subject", input.TargetSubject)
	putString(metadata, "format", input.Format)
	putString(metadata, "source_kind", input.SourceKind)
	putString(metadata, "claim_kind", input.ClaimKind)
	if input.HasImportLevel {
		metadata["import_level"] = input.ImportLevel
	}

	hints := normalizedIdentityHints(input.IdentityHints)
	if len(hints) > 0 {
		metadata["identity_hints"] = hints
	}

	return metadata
}

func RootSubject(metadata map[string]any) string {
	return stringMetadata(metadata, "root_subject")
}

func TargetSubject(metadata map[string]any) string {
	return stringMetadata(metadata, "target_subject")
}

func Format(metadata map[string]any) string {
	return stringMetadata(metadata, "format")
}

func ContactSubjects(statements []Statement) map[string]bool {
	contacts := map[string]bool{}

	for _, statement := range statements {
		if statement.Predicate != rdfTypePredicate {
			continue
		}

		if IsContactEntityType(statement.Object) {
			contacts[statement.Subject] = true
		}
	}

	return contacts
}

func IsContactEntityType(value string) bool {
	switch canonicalSchemaIRI(value) {
	case "http://xmlns.com/foaf/0.1/Person",
		"http://xmlns.com/foaf/0.1/Organization",
		"http://xmlns.com/foaf/0.1/Group",
		schemaOrgPerson,
		schemaOrgOrganization,
		"http://www.w3.org/2000/10/swap/pim/gedcom#Individual",
		"http://www.w3.org/2000/10/swap/pim/gedcom#Family",
		"http://www.w3.org/2006/vcard/ns#Individual",
		"http://www.w3.org/2006/vcard/ns#Organization",
		"http://www.w3.org/ns/org#Organization":
		return true
	default:
		return false
	}
}

func FactsFromStatements(statements []Statement, annotations []Annotation) []Fact {
	contacts := ContactSubjects(statements)

	return FactsForContacts(statements, annotations, contacts)
}

func FactsForContacts(
	statements []Statement,
	annotations []Annotation,
	contacts map[string]bool,
) []Fact {
	facts := []Fact{}
	annotationIndex := annotationsByStatement(annotations)
	nodeOwner := locationNodeOwners(statements, contacts)
	suppressed := suppressedNameNodes(statements)

	for _, statement := range statements {
		// A sub-node's literal facts (address components, coordinates, name parts) are
		// attributed to the contact that links it; a fact on the contact itself keeps
		// its own subject. Everything else off-contact is ignored.
		contactID := statement.Subject
		if !contacts[contactID] {
			owner, linked := nodeOwner[statement.Subject]
			if !linked {
				continue
			}
			contactID = owner
		}

		// Deadname suppression (gmeow:fnSelectDisplayName): a gmeow:PersonName flagged
		// gmeow:displayable false is the ONLY name filter — its facts never surface.
		if suppressed[statement.Subject] {
			continue
		}

		factKind, historical, ok := FactKind(statement.Predicate)
		if !ok {
			continue
		}

		value := FactValue(statement.Object, statement.ObjectKind, factKind)
		if strings.TrimSpace(value) == "" {
			continue
		}

		fact := Fact{
			SourceDigest:  statement.SourceDigest,
			StatementHash: statement.StatementHash,
			ContactID:     contactID,
			FactKind:      factKind,
			Value:         value,
			Predicate:     statement.Predicate,
			Historical:    historical,
		}

		for _, annotation := range annotationIndex[statementKey{
			sourceDigest:  statement.SourceDigest,
			statementHash: statement.StatementHash,
		}] {
			applyTemporalAnnotation(&fact, annotation)
		}

		facts = append(facts, fact)
	}

	facts = append(facts, nicknameAliasFacts(statements, nodeOwner, suppressed)...)

	return facts
}

// nicknameAliasFacts surfaces a nickname (a typed gmeow:NamePart whose namePartType
// is namePartNickname) as an alias of the contact bearing the appellation — the
// two-hop entity → gmeow:PersonName → gmeow:NamePart path the one-hop sub-node
// flattening does not reach. Given/surname/honorific parts are NOT surfaced: the
// gmeow:fullName already carries the display name.
func nicknameAliasFacts(
	statements []Statement,
	nameOwner map[string]string,
	suppressed map[string]bool,
) []Fact {
	// appellation node -> [namePart nodes]; namePart -> {type, text, source/hash}.
	isNickPart := map[string]bool{}
	partParent := map[string]string{}
	type partLit struct {
		text   string
		digest contracts.ObjectDigest
		hash   string
	}
	partText := map[string]partLit{}

	for _, s := range statements {
		switch s.Predicate {
		case gmeowPrefix + "hasNamePart":
			if s.ObjectKind == "iri" {
				partParent[s.Object] = s.Subject
			}
		case gmeowPrefix + "namePartType":
			if s.Object == gmeowPrefix+"namePartNickname" {
				isNickPart[s.Subject] = true
			}
		case gmeowPrefix + "partText":
			partText[s.Subject] = partLit{s.Object, s.SourceDigest, s.StatementHash}
		}
	}

	facts := []Fact{}
	for part, lit := range partText {
		if !isNickPart[part] {
			continue
		}
		appellation := partParent[part]
		if suppressed[appellation] {
			continue
		}
		contact, ok := nameOwner[appellation]
		if !ok {
			continue
		}
		value := FactValue(lit.text, "literal", FactKindAlias)
		if strings.TrimSpace(value) == "" {
			continue
		}
		facts = append(facts, Fact{
			SourceDigest:  lit.digest,
			StatementHash: lit.hash,
			ContactID:     contact,
			FactKind:      FactKindAlias,
			Value:         value,
			Predicate:     gmeowPrefix + "partText",
		})
	}

	return facts
}

// locationNodeOwners maps each address/place sub-node IRI to the contact that
// links it (via gmeow:hasContactPoint / gmeow:locatedAt — detected structurally by
// the sub-node IRI prefix, robust to the link predicate name). Sub-nodes that no
// contact links are absent, so their literals never leak into the projection.
func locationNodeOwners(
	statements []Statement,
	contacts map[string]bool,
) map[string]string {
	owners := map[string]string{}

	for _, statement := range statements {
		if !contacts[statement.Subject] {
			continue
		}
		if statement.ObjectKind != "iri" {
			continue
		}
		if strings.HasPrefix(statement.Object, gmeowAddrNodePrefix) ||
			strings.HasPrefix(statement.Object, gmeowPlaceNodePrefix) ||
			strings.HasPrefix(statement.Object, gmeowNameNodePrefix) {
			owners[statement.Object] = statement.Subject
		}
	}

	return owners
}

// suppressedNameNodes is the set of gmeow:PersonName nodes flagged
// gmeow:displayable false (deadnames / superseded names). Their facts are withheld
// from the projection — the only sanctioned name filter (fnSelectDisplayName).
func suppressedNameNodes(statements []Statement) map[string]bool {
	out := map[string]bool{}
	for _, statement := range statements {
		if statement.Predicate == gmeowPrefix+"displayable" &&
			strings.EqualFold(strings.TrimSpace(statement.Object), "false") {
			out[statement.Subject] = true
		}
	}

	return out
}

func FactKind(predicate string) (string, bool, bool) {
	kind, found := relationshipContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	kind, found = owlContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	kind, found = vcardContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	kind, found = orgContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	kind, found = foafContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	kind, historical, found := gmeowContactFactKind(predicate)
	if found {
		return kind, historical, true
	}

	kind, found = schemaContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	return "", false, false
}

func FactValue(value, objectKind, factKind string) string {
	if factKind == FactKindEmail {
		return NormalizeIdentity(value)
	}
	if factKind == FactKindContactAlias {
		return NormalizeAlias(value)
	}

	return strings.TrimSpace(value)
}

func NormalizeAlias(value string) string {
	value = strings.ToLower(strings.TrimSpace(value))
	if value == "" {
		return ""
	}

	var builder strings.Builder
	previousSeparator := false
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z',
			char >= '0' && char <= '9':
			builder.WriteRune(char)
			previousSeparator = false
		case char == '-' || char == '_' || char == '.' || char == ' ':
			if builder.Len() > 0 && !previousSeparator {
				builder.WriteByte('-')
				previousSeparator = true
			}
		}
	}

	return strings.Trim(builder.String(), "-")
}

func NormalizeIdentity(value string) string {
	value = trimMailtoAddressPrefix(value)

	address, err := mail.ParseAddress(value)
	if err == nil {
		value = address.Address
	}

	value = trimMailtoPrefix(value)

	return strings.ToLower(strings.TrimSpace(value))
}

type statementKey struct {
	sourceDigest  contracts.ObjectDigest
	statementHash string
}

const rdfTypePredicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"

func annotationsByStatement(annotations []Annotation) map[statementKey][]Annotation {
	index := map[statementKey][]Annotation{}

	for _, annotation := range annotations {
		key := statementKey{
			sourceDigest:  annotation.SourceDigest,
			statementHash: annotation.StatementHash,
		}

		index[key] = append(index[key], annotation)
	}

	return index
}

// applyTemporalAnnotation populates a fact's VALID-time axis from the four-clock
// claim annotations, and ONLY the valid clock (import-provenance.md). gmeow:validFrom
// /validUntil (and the raw OWL-Time time:hasBeginning/hasEnd a grounded source may
// carry) are real tenure; everything else — assertion / carrier / transaction time,
// and the derived recordedNoLaterThan upper bound — is NOT validity and must never
// seed ValidFrom. (The previous observedAt→ValidFrom fold fabricated a valid-from
// from ingestion time for envelope formats; that bug is removed here.)
func applyTemporalAnnotation(fact *Fact, annotation Annotation) {
	switch {
	case strings.HasSuffix(annotation.Predicate, "hasBeginning") ||
		strings.HasSuffix(annotation.Predicate, "validFrom"):
		fact.ValidFrom = annotation.Object
	case strings.HasSuffix(annotation.Predicate, "hasEnd") ||
		strings.HasSuffix(annotation.Predicate, "validUntil"):
		fact.ValidUntil = annotation.Object
	}
}

func relationshipContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://purl.org/vocab/relationship/childOf",
		"http://purl.org/vocab/relationship/parentOf",
		"http://purl.org/vocab/relationship/spouseOf",
		"https://blackcatinformatics.ca/gmeow/hasAgreement",
		"https://blackcatinformatics.ca/gmeow/hasMet",
		"https://blackcatinformatics.ca/gmeow/hasUsed",
		"https://blackcatinformatics.ca/gmeow/hasWorkedWith":
		return FactKindRelationship, true
	default:
		return "", false
	}
}

func owlContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://www.w3.org/2002/07/owl#sameAs",
		"http://www.w3.org/2004/02/skos/core#exactMatch":
		return FactKindIdentifier, true
	default:
		return "", false
	}
}

func vcardContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://www.w3.org/2006/vcard/ns#fn":
		return FactKindName, true
	case "http://www.w3.org/2006/vcard/ns#hasAddress":
		return FactKindAddress, true
	case "http://www.w3.org/2006/vcard/ns#hasEmail":
		return FactKindEmail, true
	case "http://www.w3.org/2006/vcard/ns#hasTelephone":
		return FactKindPhone, true
	case "http://www.w3.org/2006/vcard/ns#hasURL":
		return FactKindURL, true
	case "http://www.w3.org/2006/vcard/ns#note":
		return FactKindNote, true
	case "http://www.w3.org/2006/vcard/ns#nickname":
		return FactKindAlias, true
	default:
		return "", false
	}
}

func orgContactFactKind(predicate string) (string, bool) {
	if predicate == "http://www.w3.org/ns/org#member" {
		return FactKindAffiliation, true
	}

	return "", false
}

func foafContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://xmlns.com/foaf/0.1/account":
		return FactKindAccount, true
	case "http://xmlns.com/foaf/0.1/homepage":
		return FactKindURL, true
	case "http://xmlns.com/foaf/0.1/knows":
		return FactKindRelationship, true
	case "http://xmlns.com/foaf/0.1/mbox":
		return FactKindEmail, true
	case "http://xmlns.com/foaf/0.1/name":
		return FactKindName, true
	case "http://xmlns.com/foaf/0.1/nick":
		return FactKindAlias, true
	case "http://xmlns.com/foaf/0.1/phone":
		return FactKindPhone, true
	case "http://xmlns.com/foaf/0.1/title":
		return FactKindTitle, true
	default:
		return "", false
	}
}

func gmeowContactFactKind(predicate string) (string, bool, bool) {
	switch predicate {
	case contactAliasPredicate:
		return FactKindContactAlias, false, true
	case importancePredicate:
		return FactKindImportance, false, true
	case "https://blackcatinformatics.ca/gmeow/emailIdentity":
		return FactKindEmail, false, true
	case "https://blackcatinformatics.ca/gmeow/historicalEmail":
		return FactKindEmail, true, true
	case gmeowPrefix + "fullName":
		// The appellation's surface form is the contact's display name. Structured
		// components live on typed gmeow:NamePart nodes (projected separately — a
		// nickname becomes an alias; given/surname are covered by the full name).
		return FactKindName, false, true
	case gmeowPrefix + "streetAddress",
		gmeowPrefix + "extendedAddress",
		gmeowPrefix + "postOfficeBox",
		gmeowPrefix + "addressLocality",
		gmeowPrefix + "addressRegion",
		gmeowPrefix + "postalCode",
		gmeowPrefix + "countryCode":
		return FactKindAddress, false, true
	case gmeowPrefix + "latitude",
		gmeowPrefix + "longitude":
		return FactKindCoordinates, false, true
	case gmeowPrefix + "timezone":
		return FactKindTimezone, false, true
	default:
		return "", false, false
	}
}

func schemaContactFactKind(predicate string) (string, bool) {
	predicate = canonicalSchemaIRI(predicate)

	if kind, found := schemaContactFactKindEarly(predicate); found {
		return kind, true
	}

	return schemaContactFactKindLate(predicate)
}

func schemaContactFactKindEarly(predicate string) (string, bool) {
	switch predicate {
	case schemaOrgHTTPSPrefix + "address":
		return FactKindAddress, true
	case schemaOrgHTTPSPrefix + "affiliation":
		return FactKindAffiliation, true
	case schemaOrgHTTPSPrefix + "alternateName":
		return FactKindAlias, true
	case schemaOrgHTTPSPrefix + "email":
		return FactKindEmail, true
	case schemaOrgHTTPSPrefix + "identifier":
		return FactKindIdentifier, true
	case schemaOrgHTTPSPrefix + "description":
		return FactKindNote, true
	case schemaOrgHTTPSPrefix + "jobTitle":
		return FactKindTitle, true
	default:
		return "", false
	}
}

func schemaContactFactKindLate(predicate string) (string, bool) {
	switch predicate {
	case schemaOrgHTTPSPrefix + "knows":
		return FactKindRelationship, true
	case schemaOrgHTTPSPrefix + "memberOf":
		return FactKindAffiliation, true
	case schemaOrgHTTPSPrefix + "name":
		return FactKindName, true
	case schemaOrgHTTPSPrefix + "sameAs":
		return FactKindIdentifier, true
	case schemaOrgHTTPSPrefix + "telephone":
		return FactKindPhone, true
	case schemaOrgHTTPSPrefix + "url":
		return FactKindURL, true
	case schemaOrgHTTPSPrefix + "worksFor":
		return FactKindAffiliation, true
	default:
		return "", false
	}
}

func normalizedIdentityHints(values []string) []string {
	hints := []string{}
	seen := map[string]bool{}

	for _, value := range values {
		value = NormalizeIdentity(value)
		if value == "" || seen[value] {
			continue
		}

		seen[value] = true
		hints = append(hints, value)
	}

	return hints
}

func putString(metadata map[string]any, key, value string) {
	value = strings.TrimSpace(value)
	if value != "" {
		metadata[key] = value
	}
}

func stringMetadata(metadata map[string]any, key string) string {
	value, ok := metadata[key].(string)
	if !ok {
		return ""
	}

	return strings.TrimSpace(value)
}

func canonicalSchemaIRI(value string) string {
	value = strings.TrimSpace(value)
	if suffix, found := strings.CutPrefix(value, schemaOrgHTTPPrefix); found {
		return schemaOrgHTTPSPrefix + suffix
	}

	return value
}

func trimMailtoPrefix(value string) string {
	value = strings.TrimSpace(value)

	for strings.HasPrefix(strings.ToLower(value), mailtoPrefix) {
		value = strings.TrimSpace(value[len(mailtoPrefix):])
	}

	return value
}

func trimMailtoAddressPrefix(value string) string {
	value = strings.TrimSpace(value)

	start := strings.LastIndex(value, "<")
	if start < 0 {
		return trimMailtoPrefix(value)
	}

	address := value[start+1:]
	trimmedAddress := trimMailtoPrefix(address)
	if trimmedAddress == address {
		return value
	}

	return value[:start+1] + trimmedAddress
}
