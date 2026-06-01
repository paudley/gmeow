// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
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

	contactImportSourceKind = "contact_import"
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
	Contacts     []string `json:"contacts"`
	Created      bool     `json:"created,omitempty"`
	ObjectDigest string   `json:"object_digest,omitempty"`
}

type NativeBundle struct {
	Contacts      []NativeContact         `json:"contacts"`
	Metadata      map[string]any          `json:"metadata,omitempty"`
	SchemaVersion contracts.SchemaVersion `json:"schema_version"`
	GeneratedAt   time.Time               `json:"generated_at,omitzero"`
}

type NativeContact struct {
	FirstSeenAt      time.Time               `json:"first_seen_at,omitzero"`
	LastSeenAt       time.Time               `json:"last_seen_at,omitzero"`
	ContactID        string                  `json:"contact_id"`
	DisplayName      string                  `json:"display_name,omitempty"`
	PrimaryEmail     string                  `json:"primary_email,omitempty"`
	Facts            []contracts.ContactFact `json:"facts"`
	FactCount        int                     `json:"fact_count,omitempty"`
	MessageCount     int                     `json:"message_count,omitempty"`
	ParticipantCount int                     `json:"participant_count,omitempty"`
}

type vcardContact struct {
	subject string
	values  map[string][]string
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
		MediaType:   "text/turtle",
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
			switch fact.FactKind {
			case contactentity.FactKindName:
				if fact.Value != contact.DisplayName {
					writeVCardLine(&builder, "NICKNAME", fact.Value)
				}
			case contactentity.FactKindAlias:
				writeVCardLine(&builder, "NICKNAME", fact.Value)
			case contactentity.FactKindEmail:
				if !fact.Historical {
					writeVCardLine(&builder, "EMAIL", fact.Value)
				}
			case contactentity.FactKindPhone:
				writeVCardLine(&builder, "TEL", fact.Value)
			case contactentity.FactKindURL:
				writeVCardLine(&builder, "URL", fact.Value)
			case contactentity.FactKindAffiliation:
				writeVCardLine(&builder, "ORG", fact.Value)
			case contactentity.FactKindTitle:
				writeVCardLine(&builder, "TITLE", fact.Value)
			case contactentity.FactKindAddress:
				writeVCardLine(&builder, "ADR", fact.Value)
			case contactentity.FactKindNote:
				writeVCardLine(&builder, "NOTE", fact.Value)
			}
		}
		builder.WriteString("END:VCARD\n")
	}

	return builder.String()
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
		Format:     "text/turtle",
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
				Format:      "text/turtle",
				SourceKind:  contactImportSourceKind,
				ClaimKind:   format,
			}),
		},
	}
}

func nativeToRDF(content []byte) (string, []string, error) {
	var bundle NativeBundle
	if err := json.Unmarshal(content, &bundle); err != nil {
		return "", nil, fmt.Errorf("decode native contact bundle: %w", err)
	}
	if len(bundle.Contacts) == 0 {
		return "", nil, errors.New("native contact bundle has no contacts")
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
		builder.WriteString("\n")
		writeTriple(&builder, card.subject, rdfType, iri(foafPrefix+"Person"))
		for _, value := range card.values["FN"] {
			writeTriple(&builder, card.subject, foafPrefix+"name", literal(value))
		}
		for _, value := range card.values["NICKNAME"] {
			writeTriple(&builder, card.subject, foafPrefix+"nick", literal(value))
		}
		for _, value := range card.values["EMAIL"] {
			writeTriple(
				&builder,
				card.subject,
				vcardPrefix+"hasEmail",
				iri("mailto:"+contactentity.NormalizeIdentity(value)),
			)
		}
		for _, value := range card.values["TEL"] {
			writeTriple(&builder, card.subject, vcardPrefix+"hasTelephone", literal(value))
		}
		for _, value := range card.values["URL"] {
			writeTriple(&builder, card.subject, vcardPrefix+"hasURL", iri(value))
		}
		for _, value := range card.values["ORG"] {
			writeTriple(&builder, card.subject, schemaPrefix+"affiliation", literal(value))
		}
		for _, value := range card.values["TITLE"] {
			writeTriple(&builder, card.subject, schemaPrefix+"jobTitle", literal(value))
		}
		for _, value := range card.values["ADR"] {
			writeTriple(&builder, card.subject, schemaPrefix+"address", literal(value))
		}
		for _, value := range card.values["NOTE"] {
			writeTriple(&builder, card.subject, schemaPrefix+"description", literal(value))
		}
	}

	return builder.String(), uniqueStrings(contacts), nil
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
		name = strings.ToUpper(strings.TrimSpace(strings.Split(name, ";")[0]))
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

	key := firstValue(values["FN"])
	if key == "" {
		key = firstValue(values["N"])
	}
	if key == "" {
		key = "unknown"
	}

	return "urn:gmeow:contact:" + shortHash([]byte(key))
}

func unfoldVCardLines(content string) []string {
	rawLines := strings.Split(strings.ReplaceAll(content, "\r\n", "\n"), "\n")
	lines := []string{}
	for _, raw := range rawLines {
		if strings.HasPrefix(raw, " ") || strings.HasPrefix(raw, "\t") {
			if len(lines) > 0 {
				lines[len(lines)-1] += strings.TrimLeft(raw, " \t")
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

func firstRDFContactSubjects(content string) []string {
	subjects := []string{}
	for _, line := range strings.Split(content, "\n") {
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

func predicateForFact(fact contracts.ContactFact) string {
	if strings.TrimSpace(fact.Predicate) != "" {
		return fact.Predicate
	}

	switch fact.FactKind {
	case contactentity.FactKindAddress:
		return schemaPrefix + "address"
	case contactentity.FactKindAffiliation:
		return schemaPrefix + "affiliation"
	case contactentity.FactKindAlias:
		return schemaPrefix + "alternateName"
	case contactentity.FactKindEmail:
		if fact.Historical {
			return bcidPrefix + "historicalEmail"
		}

		return schemaPrefix + "email"
	case contactentity.FactKindIdentifier:
		return schemaPrefix + "identifier"
	case contactentity.FactKindName:
		return foafPrefix + "name"
	case contactentity.FactKindNote:
		return schemaPrefix + "description"
	case contactentity.FactKindPhone:
		return schemaPrefix + "telephone"
	case contactentity.FactKindRelationship:
		return schemaPrefix + "knows"
	case contactentity.FactKindTitle:
		return schemaPrefix + "jobTitle"
	case contactentity.FactKindURL:
		return schemaPrefix + "url"
	default:
		return ""
	}
}

func objectForFact(fact contracts.ContactFact) string {
	value := strings.TrimSpace(fact.Value)
	switch fact.FactKind {
	case contactentity.FactKindEmail:
		return iri("mailto:" + contactentity.NormalizeIdentity(value))
	case contactentity.FactKindURL,
		contactentity.FactKindIdentifier,
		contactentity.FactKindRelationship:
		if strings.Contains(value, ":") {
			return iri(value)
		}
	}

	return literal(value)
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
	return "<" + strings.TrimSpace(value) + ">"
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
