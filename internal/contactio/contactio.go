// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
)

const (
	FormatFOAF   = "foaf"
	FormatNative = "native"
	FormatVCard  = "vcard"

	ImportSourceKind        = "contact_import"
	contactImportSourceKind = ImportSourceKind
	MediaTypeTurtle         = "text/turtle"
)

const (
	bcidPrefix   = "https://patrickaudley.com/lod#"
	foafPrefix   = "http://xmlns.com/foaf/0.1/"
	rdfType      = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	schemaPrefix = "https://schema.org/"
	timePrefix   = "http://www.w3.org/2006/time#"
	vcardPrefix  = "http://www.w3.org/2006/vcard/ns#"
	xsdDate      = "http://www.w3.org/2001/XMLSchema#date"
)

type ImportObject struct {
	ObservedAt  time.Time
	Content     string
	MediaType   string
	SourceKind  string
	SourceName  string
	ExternalID  string
	ExternalVer string
	SourceHint  string
	Facets      []contracts.Facet
}

type ImportResult struct {
	SourceKind   string   `json:"source_kind"`
	SourceName   string   `json:"source_name"`
	ExternalID   string   `json:"external_id"`
	Format       string   `json:"format"`
	ObjectDigest string   `json:"object_digest,omitempty"`
	Error        string   `json:"error,omitempty"`
	Contacts     []string `json:"contacts"`
	Created      bool     `json:"created,omitempty"`
}

type NativeBundle struct {
	GeneratedAt   time.Time               `json:"generated_at,omitzero"`
	Metadata      map[string]any          `json:"metadata,omitempty"`
	Contacts      []NativeContact         `json:"contacts"`
	SchemaVersion contracts.SchemaVersion `json:"schema_version"`
}

type NativeContact struct {
	FirstSeenAt      time.Time               `json:"first_seen_at,omitzero"`
	LastSeenAt       time.Time               `json:"last_seen_at,omitzero"`
	Aliases          []string                `json:"aliases,omitempty"`
	ContactID        string                  `json:"contact_id"`
	DisplayName      string                  `json:"display_name,omitempty"`
	PrimaryEmail     string                  `json:"primary_email,omitempty"`
	Facts            []contracts.ContactFact `json:"facts"`
	FactCount        int                     `json:"fact_count,omitempty"`
	MessageCount     int                     `json:"message_count,omitempty"`
	ParticipantCount int                     `json:"participant_count,omitempty"`
}

type vcardContact struct {
	values  map[string][]string
	subject string
}

func BuildImportObject(
	format string,
	sourceName string,
	path string,
	content []byte,
	observedAt time.Time,
) (ImportObject, ImportResult, error) {
	format = NormalizeFormat(format)

	if sourceName = strings.TrimSpace(sourceName); sourceName == "" {
		return ImportObject{}, ImportResult{}, errors.New("source name is required")
	}

	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	rendered, contacts, err := importContent(format, content)
	if err != nil {
		return ImportObject{}, ImportResult{}, err
	}

	externalID := strings.TrimSpace(filepath.ToSlash(path))
	if externalID == "" {
		externalID = format + "-" + shortHash(content)
	}

	externalVersion := contentHash(content)
	facets := importFacets(format, contacts)

	object := ImportObject{
		ObservedAt:  observedAt.UTC(),
		Content:     rendered,
		MediaType:   MediaTypeTurtle,
		SourceKind:  contactImportSourceKind,
		SourceName:  sourceName,
		ExternalID:  externalID,
		ExternalVer: externalVersion,
		SourceHint:  externalID,
		Facets:      facets,
	}
	result := ImportResult{
		SourceKind: object.SourceKind,
		SourceName: object.SourceName,
		ExternalID: object.ExternalID,
		Format:     format,
		Contacts:   contacts,
	}

	return object, result, nil
}

func Export(format string, contacts []contracts.ContactAggregate) (string, error) {
	switch NormalizeFormat(format) {
	case FormatFOAF:
		return ExportFOAF(contacts), nil
	case FormatNative:
		return ExportNative(contacts)
	case FormatVCard:
		return ExportVCard(contacts), nil
	default:
		return "", fmt.Errorf("unsupported contact export format %q", format)
	}
}

func ExportVCard(contacts []contracts.ContactAggregate) string {
	var builder strings.Builder

	for _, contact := range contacts {
		builder.WriteString("BEGIN:VCARD\nVERSION:4.0\n")
		writeVCardLine(&builder, "FN", firstNonEmpty(contact.DisplayName, contact.ContactID))

		for _, fact := range exportFacts(contact.Facts) {
			writeContactFactVCardLine(&builder, fact, contact.DisplayName)
		}

		builder.WriteString("END:VCARD\n")
	}

	return builder.String()
}

func writeContactFactVCardLine(
	builder *strings.Builder,
	fact contracts.ContactFact,
	displayName string,
) {
	if fact.FactKind == contactentity.FactKindName {
		if fact.Value != displayName {
			writeVCardLine(builder, "NICKNAME", fact.Value)
		}

		return
	}

	if fact.FactKind == contactentity.FactKindEmail {
		if !fact.Historical {
			writeVCardLine(builder, "EMAIL", fact.Value)
		}

		return
	}

	if property := factVCardProperties[fact.FactKind]; property != "" {
		writeVCardLine(builder, property, fact.Value)
	}
}

var factVCardProperties = map[string]string{
	contactentity.FactKindAddress:     "ADR",
	contactentity.FactKindAffiliation: "ORG",
	contactentity.FactKindAlias:       "NICKNAME",
	contactentity.FactKindNote:        "NOTE",
	contactentity.FactKindPhone:       "TEL",
	contactentity.FactKindTitle:       "TITLE",
	contactentity.FactKindURL:         "URL",
}

func ExportFOAF(contacts []contracts.ContactAggregate) string {
	var builder strings.Builder
	writePrefixes(&builder)

	for _, contact := range contacts {
		subject := contact.ContactID
		if subject == "" {
			continue
		}

		builder.WriteString("\n")
		writeTriple(&builder, subject, rdfType, iri(foafPrefix+"Person"))

		for _, fact := range exportFacts(contact.Facts) {
			predicate := predicateForFact(fact)
			if predicate == "" {
				continue
			}

			writeTriple(&builder, subject, predicate, objectForFact(fact))
			writeTemporalAnnotations(&builder, subject, predicate, objectForFact(fact), fact)
		}
	}

	return builder.String()
}

func ExportNative(contacts []contracts.ContactAggregate) (string, error) {
	bundle := NativeBundle{
		SchemaVersion: contracts.SchemaVersionPhase00,
		GeneratedAt:   time.Now().UTC(),
		Contacts:      make([]NativeContact, 0, len(contacts)),
	}
	for _, contact := range contacts {
		bundle.Contacts = append(bundle.Contacts, NativeContact{
			ContactID:        contact.ContactID,
			DisplayName:      contact.DisplayName,
			PrimaryEmail:     contact.PrimaryEmail,
			Aliases:          append([]string{}, contact.Aliases...),
			FirstSeenAt:      contact.FirstSeenAt,
			LastSeenAt:       contact.LastSeenAt,
			Facts:            exportFacts(contact.Facts),
			FactCount:        contact.FactCount,
			MessageCount:     contact.MessageCount,
			ParticipantCount: contact.ParticipantCount,
		})
	}

	encoded, err := json.MarshalIndent(bundle, "", "  ")
	if err != nil {
		return "", fmt.Errorf("encode native contact bundle: %w", err)
	}

	return string(encoded) + "\n", nil
}

func NormalizeFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "foaf", "rdf", "ttl", "turtle", "text/turtle":
		return FormatFOAF
	case "json", "native", "gmeow":
		return FormatNative
	case "vcf", "vcard", "text/vcard", "text/x-vcard":
		return FormatVCard
	default:
		return strings.ToLower(strings.TrimSpace(format))
	}
}

func importContent(format string, content []byte) (string, []string, error) {
	switch format {
	case FormatFOAF:
		text := strings.TrimSpace(string(content))
		if text == "" {
			return "", nil, errors.New("FOAF/RDF import is empty")
		}

		return text + "\n", firstRDFContactSubjects(text), nil
	case FormatNative:
		return nativeToRDF(content)
	case FormatVCard:
		return vcardToRDF(string(content))
	default:
		return "", nil, fmt.Errorf("unsupported contact import format %q", format)
	}
}

func importFacets(format string, contacts []string) []contracts.Facet {
	input := contactentity.MetadataInput{
		Format:     MediaTypeTurtle,
		SourceKind: contactImportSourceKind,
	}
	if len(contacts) == 1 {
		input.RootSubject = contacts[0]
	}

	return []contracts.Facet{
		contactentity.Facet(input),
		{
			Kind: contracts.RDFSourceBundleFacetKind,
			Metadata: contactentity.Metadata(contactentity.MetadataInput{
				RootSubject: input.RootSubject,
				Format:      MediaTypeTurtle,
				SourceKind:  contactImportSourceKind,
				ClaimKind:   format,
			}),
		},
	}
}

func nativeToRDF(content []byte) (string, []string, error) {
	var bundle NativeBundle
	err := json.Unmarshal(content, &bundle)
	if err != nil {
		return "", nil, fmt.Errorf("decode native contact bundle: %w", err)
	}

	if len(bundle.Contacts) == 0 {
		return "", nil, errors.New("native contact bundle has no contacts")
	}

	if bundle.SchemaVersion != contracts.SchemaVersionPhase00 {
		return "", nil, fmt.Errorf(
			"unsupported native contact schema_version %d",
			bundle.SchemaVersion,
		)
	}

	var builder strings.Builder
	writePrefixes(&builder)

	contacts := make([]string, 0, len(bundle.Contacts))
	for _, contact := range bundle.Contacts {
		if strings.TrimSpace(contact.ContactID) == "" {
			return "", nil, errors.New("native contact is missing contact_id")
		}

		contacts = append(contacts, contact.ContactID)

		builder.WriteString("\n")
		writeTriple(&builder, contact.ContactID, rdfType, iri(foafPrefix+"Person"))

		if contact.DisplayName != "" {
			writeTriple(
				&builder,
				contact.ContactID,
				foafPrefix+"name",
				literal(contact.DisplayName),
			)
		}

		if contact.PrimaryEmail != "" {
			writeTriple(
				&builder,
				contact.ContactID,
				schemaPrefix+"email",
				iri("mailto:"+contactentity.NormalizeIdentity(contact.PrimaryEmail)),
			)
		}

		for _, alias := range contact.Aliases {
			normalized := contactentity.NormalizeAlias(alias)
			if normalized == "" {
				continue
			}
			writeTriple(
				&builder,
				contact.ContactID,
				bcidPrefix+"contactAlias",
				literal(normalized),
			)
		}

		for _, fact := range contact.Facts {
			predicate := predicateForFact(fact)
			if predicate == "" || strings.TrimSpace(fact.Value) == "" {
				continue
			}

			object := objectForFact(fact)
			writeTriple(&builder, contact.ContactID, predicate, object)
			writeTemporalAnnotations(&builder, contact.ContactID, predicate, object, fact)
		}
	}

	return builder.String(), uniqueStrings(contacts), nil
}

func vcardToRDF(content string) (string, []string, error) {
	cards, err := parseVCards(content)
	if err != nil {
		return "", nil, err
	}

	if len(cards) == 0 {
		return "", nil, errors.New("vCard import has no cards")
	}

	var builder strings.Builder
	writePrefixes(&builder)

	contacts := make([]string, 0, len(cards))
	for _, card := range cards {
		contacts = append(contacts, card.subject)
		writeVCardContactRDF(&builder, card)
	}

	return builder.String(), uniqueStrings(contacts), nil
}

func writeVCardContactRDF(builder *strings.Builder, card vcardContact) {
	builder.WriteString("\n")
	writeTriple(builder, card.subject, rdfType, iri(foafPrefix+"Person"))
	writeVCardValueTriples(builder, card, "FN", foafPrefix+"name", literal)
	writeVCardValueTriples(builder, card, "NICKNAME", foafPrefix+"nick", literal)
	writeVCardValueTriples(
		builder,
		card,
		"EMAIL",
		vcardPrefix+"hasEmail",
		normalizedEmailIRI,
	)
	writeVCardValueTriples(builder, card, "TEL", vcardPrefix+"hasTelephone", literal)
	writeVCardValueTriples(builder, card, "URL", vcardPrefix+"hasURL", iri)
	writeVCardValueTriples(builder, card, "ORG", schemaPrefix+"affiliation", literal)
	writeVCardValueTriples(builder, card, "TITLE", schemaPrefix+"jobTitle", literal)
	writeVCardValueTriples(builder, card, "ADR", schemaPrefix+"address", literal)
	writeVCardValueTriples(builder, card, "NOTE", schemaPrefix+"description", literal)
}

func writeVCardValueTriples(
	builder *strings.Builder,
	card vcardContact,
	name string,
	predicate string,
	object func(string) string,
) {
	for _, value := range card.values[name] {
		writeTriple(builder, card.subject, predicate, object(value))
	}
}

func parseVCards(content string) ([]vcardContact, error) {
	lines := unfoldVCardLines(content)
	cards := []vcardContact{}

	var current map[string][]string

	for _, line := range lines {
		name, value, found := strings.Cut(line, ":")
		if !found {
			continue
		}

		name = vcardPropertyName(name)
		value = unescapeVCardValue(value)

		switch name {
		case "BEGIN":
			if strings.EqualFold(value, "VCARD") {
				current = map[string][]string{}
			}
		case "END":
			if strings.EqualFold(value, "VCARD") && current != nil {
				cards = append(cards, vcardContact{
					subject: contactSubjectForVCard(current),
					values:  current,
				})
				current = nil
			}
		default:
			if current != nil && value != "" {
				current[name] = append(current[name], value)
			}
		}
	}

	if current != nil {
		return nil, errors.New("unterminated vCard")
	}

	return cards, nil
}

func contactSubjectForVCard(values map[string][]string) string {
	for _, email := range values["EMAIL"] {
		normalized := contactentity.NormalizeIdentity(email)
		if normalized != "" {
			return "mailto:" + normalized
		}
	}

	key := firstNonEmpty(
		prefixedFirstValue("UID", values["UID"]),
		prefixedFirstValue("FN", values["FN"]),
		prefixedFirstValue("N", values["N"]),
	)
	if key == "" {
		key = stableVCardFingerprint(values)
	}

	return "urn:gmeow:contact:" + shortHash([]byte(key))
}

func prefixedFirstValue(prefix string, values []string) string {
	if value := firstValue(values); value != "" {
		return prefix + "=" + value
	}

	return ""
}

func stableVCardFingerprint(values map[string][]string) string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}

	sort.Strings(keys)

	parts := make([]string, 0, len(keys))
	for _, key := range keys {
		parts = append(parts, key+"="+strings.Join(values[key], "|"))
	}

	return strings.Join(parts, ";")
}

func unfoldVCardLines(content string) []string {
	rawLines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	lines := []string{}

	for _, raw := range rawLines {
		if strings.HasPrefix(raw, " ") || strings.HasPrefix(raw, "\t") {
			if len(lines) > 0 {
				lines[len(lines)-1] += raw[1:]
			}

			continue
		}

		if strings.TrimSpace(raw) != "" {
			lines = append(lines, strings.TrimRight(raw, "\r"))
		}
	}

	return lines
}

func unescapeVCardValue(value string) string {
	replacer := strings.NewReplacer(
		`\n`,
		"\n",
		`\N`,
		"\n",
		`\,`,
		",",
		`\;`,
		";",
		`\\`,
		`\`,
	)

	return strings.TrimSpace(replacer.Replace(value))
}

func vcardPropertyName(value string) string {
	property := strings.Split(value, ";")[0]
	if _, after, found := strings.Cut(property, "."); found {
		property = after
	}

	return strings.ToUpper(strings.TrimSpace(property))
}

func firstRDFContactSubjects(content string) []string {
	subjects := []string{}

	for line := range strings.SplitSeq(content, "\n") {
		line = strings.TrimSpace(line)
		if !strings.HasPrefix(line, "<") {
			continue
		}

		subject, rest, found := strings.Cut(line, ">")
		if !found {
			continue
		}

		if strings.Contains(rest, "foaf:Person") ||
			strings.Contains(rest, "schema:Person") ||
			strings.Contains(rest, "<"+foafPrefix+"Person>") ||
			strings.Contains(rest, "<"+schemaPrefix+"Person>") {
			subjects = append(subjects, strings.TrimPrefix(subject, "<"))
		}
	}

	return uniqueStrings(subjects)
}

func exportFacts(facts []contracts.ContactFact) []contracts.ContactFact {
	copied := append([]contracts.ContactFact{}, facts...)
	sort.SliceStable(copied, func(left, right int) bool {
		if copied[left].ContactID != copied[right].ContactID {
			return copied[left].ContactID < copied[right].ContactID
		}

		if copied[left].FactKind != copied[right].FactKind {
			return copied[left].FactKind < copied[right].FactKind
		}

		if copied[left].Value != copied[right].Value {
			return copied[left].Value < copied[right].Value
		}

		return copied[left].StatementHash < copied[right].StatementHash
	})

	return copied
}

var factPredicates = map[string]string{
	contactentity.FactKindAddress:      schemaPrefix + "address",
	contactentity.FactKindAffiliation:  schemaPrefix + "affiliation",
	contactentity.FactKindAlias:        schemaPrefix + "alternateName",
	contactentity.FactKindContactAlias: bcidPrefix + "contactAlias",
	contactentity.FactKindIdentifier:   schemaPrefix + "identifier",
	contactentity.FactKindName:         foafPrefix + "name",
	contactentity.FactKindNote:         schemaPrefix + "description",
	contactentity.FactKindPhone:        schemaPrefix + "telephone",
	contactentity.FactKindRelationship: schemaPrefix + "knows",
	contactentity.FactKindTitle:        schemaPrefix + "jobTitle",
	contactentity.FactKindURL:          schemaPrefix + "url",
}

func predicateForFact(fact contracts.ContactFact) string {
	if strings.TrimSpace(fact.Predicate) != "" {
		return fact.Predicate
	}

	if fact.FactKind == contactentity.FactKindEmail {
		if fact.Historical {
			return bcidPrefix + "historicalEmail"
		}

		return schemaPrefix + "email"
	}

	return factPredicates[fact.FactKind]
}

func objectForFact(fact contracts.ContactFact) string {
	value := strings.TrimSpace(fact.Value)
	switch fact.FactKind {
	case contactentity.FactKindEmail:
		return normalizedEmailIRI(value)
	case contactentity.FactKindURL,
		contactentity.FactKindIdentifier,
		contactentity.FactKindRelationship:
		if strings.HasPrefix(value, "_:") {
			return blankNodeOrLiteral(value)
		}

		if strings.Contains(value, ":") {
			return iri(value)
		}
	}

	return literal(value)
}

func normalizedEmailIRI(value string) string {
	return iri("mailto:" + contactentity.NormalizeIdentity(value))
}

func blankNodeOrLiteral(value string) string {
	label := strings.TrimPrefix(value, "_:")
	if label == "" {
		return literal(value)
	}

	for _, char := range label {
		if char == '_' || char == '-' || char >= '0' && char <= '9' ||
			char >= 'A' && char <= 'Z' || char >= 'a' && char <= 'z' {
			continue
		}

		return literal(value)
	}

	return "_:" + label
}

func writePrefixes(builder *strings.Builder) {
	builder.WriteString("@prefix bcid: <" + bcidPrefix + "> .\n")
	builder.WriteString("@prefix foaf: <" + foafPrefix + "> .\n")
	builder.WriteString("@prefix schema: <" + schemaPrefix + "> .\n")
	builder.WriteString("@prefix time: <" + timePrefix + "> .\n")
	builder.WriteString("@prefix vcard: <" + vcardPrefix + "> .\n")
	builder.WriteString("@prefix xsd: <http://www.w3.org/2001/XMLSchema#> .\n")
}

func writeTriple(builder *strings.Builder, subject, predicate, object string) {
	builder.WriteString(iri(subject))
	builder.WriteString(" ")
	builder.WriteString(iri(predicate))
	builder.WriteString(" ")
	builder.WriteString(object)
	builder.WriteString(" .\n")
}

func writeTemporalAnnotations(
	builder *strings.Builder,
	subject string,
	predicate string,
	object string,
	fact contracts.ContactFact,
) {
	if fact.ValidFrom == "" && fact.ValidUntil == "" {
		return
	}

	statement := "<< " + iri(subject) + " " + iri(predicate) + " " + object + " >> "
	if fact.ValidFrom != "" {
		builder.WriteString(statement)
		builder.WriteString(iri(timePrefix + "hasBeginning"))
		builder.WriteString(" ")
		builder.WriteString(typedDate(fact.ValidFrom))
		builder.WriteString(" .\n")
	}

	if fact.ValidUntil != "" {
		builder.WriteString(statement)
		builder.WriteString(iri(timePrefix + "hasEnd"))
		builder.WriteString(" ")
		builder.WriteString(typedDate(fact.ValidUntil))
		builder.WriteString(" .\n")
	}
}

func writeVCardLine(builder *strings.Builder, name, value string) {
	value = strings.TrimSpace(value)
	if value == "" {
		return
	}

	builder.WriteString(name)
	builder.WriteString(":")
	builder.WriteString(escapeVCardValue(value))
	builder.WriteString("\n")
}

func escapeVCardValue(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, "\n", `\n`, ";", `\;`, ",", `\,`)

	return replacer.Replace(value)
}

func iri(value string) string {
	value = strings.TrimSpace(value)
	if strings.HasPrefix(value, "<") && strings.HasSuffix(value, ">") {
		value = strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(value, "<"), ">"))
	}

	return "<" + encodeIRI(value) + ">"
}

func encodeIRI(value string) string {
	var builder strings.Builder

	for _, char := range value {
		switch {
		case char <= 0x20 || char == 0x7f ||
			strings.ContainsRune("<>\"{}|\\^`", char):
			builder.WriteString(url.QueryEscape(string(char)))
		default:
			builder.WriteRune(char)
		}
	}

	return strings.ReplaceAll(builder.String(), "+", "%20")
}

func literal(value string) string {
	return `"` + escapeLiteral(value) + `"`
}

func typedDate(value string) string {
	return literal(value) + "^^" + iri(xsdDate)
}

func escapeLiteral(value string) string {
	replacer := strings.NewReplacer(`\`, `\\`, `"`, `\"`, "\n", `\n`)

	return replacer.Replace(strings.TrimSpace(value))
}

func firstValue(values []string) string {
	if len(values) == 0 {
		return ""
	}

	return strings.TrimSpace(values[0])
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}

	return ""
}

func contentHash(content []byte) string {
	sum := sha256.Sum256(content)

	return hex.EncodeToString(sum[:])
}

func shortHash(content []byte) string {
	return contentHash(content)[:16]
}

func uniqueStrings(values []string) []string {
	seen := map[string]bool{}
	result := []string{}

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}

		seen[value] = true
		result = append(result, value)
	}

	return result
}
