// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"path/filepath"
	"sort"
	"strings"
	"time"
	"unicode"
	"unicode/utf16"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
)

const (
	FormatAppleAddressBook      = "apple-addressbook"
	FormatAppleAddressBookGroup = "apple-addressbook-group"
	FormatBBDB                  = "bbdb"
	FormatCSV                   = "csv"
	FormatRDF                   = "rdf"
	FormatGEDCOM                = "gedcom"
	FormatNative                = "native"
	FormatVCard                 = "vcard"

	ImportSourceKind        = "contact_import"
	contactImportSourceKind = ImportSourceKind
	MediaTypeTurtle         = "text/turtle"
)

const (
	gmeowPrefix  = "https://blackcatinformatics.ca/gmeow/"
	foafPrefix   = "http://xmlns.com/foaf/0.1/"
	rdfType      = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	schemaPrefix = "https://schema.org/"
	timePrefix   = "http://www.w3.org/2006/time#"
	vcardPrefix  = "http://www.w3.org/2006/vcard/ns#"
	xsdDate      = "http://www.w3.org/2001/XMLSchema#date"
	xsdDateTime  = "http://www.w3.org/2001/XMLSchema#dateTime"
	xsdInteger   = "http://www.w3.org/2001/XMLSchema#integer"
	xsdDecimal   = "http://www.w3.org/2001/XMLSchema#decimal"
)

const relPrefix = "http://purl.org/vocab/relationship/"

// ErrSkipNonContactDomain marks input that parsed cleanly but is deliberately
// out of the contact domain (e.g. an Apple smart group / saved search). Callers
// may treat it as a skip rather than an ingestion failure.
var ErrSkipNonContactDomain = errors.New("record is not contact-domain")

const (
	MinImportLevel = 0
	MaxImportLevel = 10

	GmeowImportanceLevel = gmeowPrefix + "importanceLevel"
	GmeowHasAgreement    = gmeowPrefix + "hasAgreement"
	GmeowHasMet          = gmeowPrefix + "hasMet"
	GmeowHasUsed         = gmeowPrefix + "hasUsed"
	GmeowHasWorkedWith   = gmeowPrefix + "hasWorkedWith"
)

type ImportObject struct {
	ObservedAt  time.Time
	Content     string
	MediaType   string
	SourceKind  string
	SourceName  string
	ExternalID  string
	ExternalVer string
	ImportLevel int
	SourceHint  string
	Facets      []contracts.Facet
}

type ImportResult struct {
	SourceKind   string            `json:"source_kind"`
	SourceName   string            `json:"source_name"`
	ExternalID   string            `json:"external_id"`
	Format       string            `json:"format"`
	ImportLevel  int               `json:"import_level"`
	ObjectDigest string            `json:"object_digest,omitempty"`
	Error        string            `json:"error,omitempty"`
	Contacts     []string          `json:"contacts"`
	Rejected     []RecordRejection `json:"rejected,omitempty"`
	Created      bool              `json:"created,omitempty"`
}

// ContactDelta is one logical contact rendered as a standalone RDF/Turtle
// object, keyed by its own identity. Phase 3 stores the contact — not the
// container file — so each delta is ingested as its own FILESTORE object and
// repeated snapshots of the same contact dedup/version by identity.
type ContactDelta struct {
	Identity  string
	Content   string
	MediaType string
	Facets    []contracts.Facet
}

// renderedContact is the per-contact body (no @prefix header) produced by a
// format parser, plus the logical identity it belongs to. The bundle and the
// per-contact deltas are both assembled from these.
type renderedContact struct {
	identity string
	body     string
}

// RecordRejection records a single logical record that was rejected during
// import without failing the rest of the file — the per-record half of the
// import-run manifest. A record is rejected when one of its source properties
// has no mapping or the record is structurally damaged; nothing is written for
// it, but the reason is preserved for audit.
type RecordRejection struct {
	Format  string `json:"format"`
	Reason  string `json:"reason"`
	Subject string `json:"subject,omitempty"`
	Index   int    `json:"index"`
}

type ImportOptions struct {
	ImportLevel int
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
	lines   []vcardLine
	values  map[string][]string
	subject string
}

type vcardLine struct {
	name   string
	params map[string][]string
	value  string
}

type bbdbContact struct {
	addresses   []bbdbAddress
	aliases     []string
	company     string
	displayName string
	emails      []string
	phones      []bbdbPhone
	userFields  []bbdbUserField
	subject     string
}

type bbdbAddress struct {
	Label    string
	Streets  []string
	City     string
	State    string
	Postcode string
	Country  string
}

type bbdbPhone struct {
	Label      string
	Number     string
	Area       string
	Exchange   string
	Subscriber string
	Extension  string
}

type bbdbUserField struct {
	name  string
	value string
}

type providerLifecycle struct {
	ValidUntil string
	Confidence string
	SourceURL  string
	Caveat     string
}

type bbdbValue struct {
	symbol string
	text   string
	items  []bbdbValue
	quoted bool
}

type bbdbParser struct {
	input string
	index int
}

type csvRow struct {
	header []string
	values map[string]string
}

type appleScalarField struct {
	Predicate string
	Kind      string
}

type appleMultiValueField struct {
	LinkPredicate      string
	ValuePredicate     string
	ValueKind          string
	ComponentPredicate map[string]string
}

type gedcomLine struct {
	level int
	xref  string
	tag   string
	value string
}

type gedcomIndividual struct {
	xref  string
	facts map[string][]gedcomFact
}

type gedcomFamily struct {
	xref  string
	facts map[string][]gedcomFact
}

type gedcomFact struct {
	tag      string
	value    string
	children []gedcomFact
}

func BuildImportObject(
	format string,
	sourceName string,
	path string,
	content []byte,
	observedAt time.Time,
) (ImportObject, ImportResult, error) {
	return BuildImportObjectWithOptions(
		format,
		sourceName,
		path,
		content,
		observedAt,
		ImportOptions{ImportLevel: MinImportLevel},
	)
}

func BuildImportObjectWithOptions(
	format string,
	sourceName string,
	path string,
	content []byte,
	observedAt time.Time,
	options ImportOptions,
) (ImportObject, ImportResult, error) {
	format = NormalizeFormat(format)

	if sourceName = strings.TrimSpace(sourceName); sourceName == "" {
		return ImportObject{}, ImportResult{}, errors.New("source name is required")
	}
	if err := ValidateImportLevel(options.ImportLevel); err != nil {
		return ImportObject{}, ImportResult{}, err
	}

	if observedAt.IsZero() {
		observedAt = time.Now().UTC()
	}

	records, contacts, rejections, err := importContent(format, content)
	if err != nil {
		return ImportObject{}, ImportResult{}, err
	}

	externalID := strings.TrimSpace(filepath.ToSlash(path))
	if externalID == "" {
		externalID = format + "-" + shortHash(content)
	}

	externalVersion := contentHash(content)
	facets := importFacets(format, contacts, options.ImportLevel)

	object := ImportObject{
		ObservedAt:  observedAt.UTC(),
		Content:     contactBundleContent(records, contacts, options.ImportLevel),
		MediaType:   MediaTypeTurtle,
		SourceKind:  contactImportSourceKind,
		SourceName:  sourceName,
		ExternalID:  externalID,
		ExternalVer: externalVersion,
		ImportLevel: options.ImportLevel,
		SourceHint:  externalID,
		Facets:      facets,
	}
	result := ImportResult{
		SourceKind:  object.SourceKind,
		SourceName:  object.SourceName,
		ExternalID:  object.ExternalID,
		Format:      format,
		ImportLevel: options.ImportLevel,
		Contacts:    contacts,
		Rejected:    rejections,
	}

	return object, result, nil
}

// BuildContactDeltas parses one input into per-logical-contact delta objects —
// the Phase 3 storage unit. Each delta is keyed by its contact identity so the
// CLI can ingest it as its own FILESTORE object (deduped/versioned by identity,
// not by container file). It shares parsing with BuildImportObjectWithOptions.
func BuildContactDeltas(
	format string,
	sourceName string,
	content []byte,
	options ImportOptions,
) ([]ContactDelta, ImportResult, error) {
	format = NormalizeFormat(format)
	if sourceName = strings.TrimSpace(sourceName); sourceName == "" {
		return nil, ImportResult{}, errors.New("source name is required")
	}
	if err := ValidateImportLevel(options.ImportLevel); err != nil {
		return nil, ImportResult{}, err
	}

	records, contacts, rejections, err := importContent(format, content)
	if err != nil {
		return nil, ImportResult{}, err
	}

	deltas := make([]ContactDelta, 0, len(records))
	for _, record := range records {
		deltas = append(deltas, ContactDelta{
			Identity:  record.identity,
			Content:   contactDeltaContent(record.identity, record.body, options.ImportLevel),
			MediaType: MediaTypeTurtle,
			// Single-contact facets so the QUERY projection recognizes the object
			// and knows its root subject — without these it extracts no facts.
			Facets: importFacets(format, []string{record.identity}, options.ImportLevel),
		})
	}

	result := ImportResult{
		SourceKind:  contactImportSourceKind,
		SourceName:  sourceName,
		Format:      format,
		ImportLevel: options.ImportLevel,
		Contacts:    contacts,
		Rejected:    rejections,
	}

	return deltas, result, nil
}

func ValidateImportLevel(level int) error {
	if level < MinImportLevel || level > MaxImportLevel {
		return fmt.Errorf(
			"contact import level must be between %d and %d",
			MinImportLevel,
			MaxImportLevel,
		)
	}

	return nil
}

func Export(format string, contacts []contracts.ContactAggregate) (string, error) {
	switch NormalizeFormat(format) {
	case FormatRDF:
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

		claims := []Claim{
			{Subject: subject, Predicate: rdfType, Object: termIRI(foafPrefix + "Person")},
		}
		for _, fact := range exportFacts(contact.Facts) {
			predicate := predicateForFact(fact)
			if predicate == "" {
				continue
			}

			claim := Claim{
				Subject:   subject,
				Predicate: predicate,
				Object:    termRaw(objectForFact(fact)),
			}
			if fact.ValidFrom != "" {
				claim = claim.withAnnotation(
					timePrefix+"hasBeginning",
					termTypedDate(fact.ValidFrom),
				)
			}
			if fact.ValidUntil != "" {
				claim = claim.withAnnotation(timePrefix+"hasEnd", termTypedDate(fact.ValidUntil))
			}
			claims = append(claims, claim)
		}

		builder.WriteString("\n")
		builder.WriteString(BuildRDFStarDelta(claims))
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

// FormatForPath infers the contact import format from a file extension, for
// importing a mixed corpus directory where each file may be a different format.
// Returns "" for extensions that are not contact-domain inputs (callers skip).
func FormatForPath(path string) string {
	switch strings.ToLower(filepath.Ext(path)) {
	case ".vcf", ".vcard":
		return FormatVCard
	case ".abcdp":
		return FormatAppleAddressBook
	case ".abcdg":
		return FormatAppleAddressBookGroup
	case ".csv":
		return FormatCSV
	case ".ttl", ".turtle", ".rdf":
		return FormatRDF
	case ".ged", ".gedcom":
		return FormatGEDCOM
	case ".bbdb":
		return FormatBBDB
	case ".json":
		return FormatNative
	default:
		return ""
	}
}

func NormalizeFormat(format string) string {
	switch strings.ToLower(strings.TrimSpace(format)) {
	case "abcdp", "abperson", "apple", "apple-addressbook", "addressbook":
		return FormatAppleAddressBook
	case "abcdg", "abgroup", "apple-group", "apple-addressbook-group":
		return FormatAppleAddressBookGroup
	case "bbdb":
		return FormatBBDB
	case "csv", "text/csv":
		return FormatCSV
	case "foaf", "rdf", "rdf/ttl", "ttl", "turtle", "text/turtle":
		return FormatRDF
	case "ged", "gedcom", "text/gedcom":
		return FormatGEDCOM
	case "json", "native", "gmeow":
		return FormatNative
	case "vcf", "vcard", "text/vcard", "text/x-vcard":
		return FormatVCard
	default:
		return strings.ToLower(strings.TrimSpace(format))
	}
}

// importContent parses one file into per-contact records (each a logical contact
// with its own RDF body), the full list of contact subjects, and any per-record
// rejections. Each record becomes its own FILESTORE object (Phase 3); the
// contacts list drives the ImportResult.
func importContent(
	format string,
	content []byte,
) ([]renderedContact, []string, []RecordRejection, error) {
	switch format {
	case FormatAppleAddressBook:
		records, rejections, err := appleAddressBookPersonToRDF(content)
		return records, recordIdentities(records), rejections, err
	case FormatAppleAddressBookGroup:
		records, rejections, err := appleAddressBookGroupToRDF(content)
		return records, recordIdentities(records), rejections, err
	case FormatBBDB:
		records, rejections, err := bbdbToRDF(importText(content))
		return records, recordIdentities(records), rejections, err
	case FormatCSV:
		records, rejections, err := csvToRDF(importDecodedText(content))
		return records, recordIdentities(records), rejections, err
	case FormatRDF:
		text := strings.TrimSpace(importText(content))
		if text == "" {
			return nil, nil, nil, errors.New("RDF/Turtle import is empty")
		}

		contacts := firstRDFContactSubjects(text)
		if len(contacts) == 0 {
			return nil, nil, nil, errors.New("RDF/Turtle import has no contacts")
		}

		// A rooted graph is one contact; an un-rooted collection lists many but
		// shares one graph body (the whole graph is preserved — semantic superset).
		record := renderedContact{identity: contacts[0], body: text}
		return []renderedContact{record}, contacts, nil, nil
	case FormatGEDCOM:
		records, rejections, err := gedcomToRDF(importDecodedText(content))
		return records, recordIdentities(records), rejections, err
	case FormatNative:
		records, rejections, err := nativeToRDF(content)
		return records, recordIdentities(records), rejections, err
	case FormatVCard:
		// Use the UTF-16-aware decoder (as CSV/GEDCOM do): many real vCard
		// exports are UTF-16, which importText would mangle into NUL-separated
		// property/parameter names.
		records, rejections, err := vcardToRDF(importDecodedText(content))
		return records, recordIdentities(records), rejections, err
	default:
		return nil, nil, nil, fmt.Errorf("unsupported contact import format %q", format)
	}
}

func recordIdentities(records []renderedContact) []string {
	identities := make([]string, 0, len(records))
	for _, record := range records {
		identities = append(identities, record.identity)
	}

	return uniqueStrings(identities)
}

func importText(content []byte) string {
	return strings.ToValidUTF8(string(content), "?")
}

func importDecodedText(content []byte) string {
	if len(content) >= 2 {
		switch {
		case content[0] == 0xff && content[1] == 0xfe:
			return decodeUTF16(content[2:], binary.LittleEndian)
		case content[0] == 0xfe && content[1] == 0xff:
			return decodeUTF16(content[2:], binary.BigEndian)
		}
	}
	if looksLikeUTF16(content, binary.LittleEndian) {
		return decodeUTF16(content, binary.LittleEndian)
	}
	if looksLikeUTF16(content, binary.BigEndian) {
		return decodeUTF16(content, binary.BigEndian)
	}

	return importText(content)
}

func looksLikeUTF16(content []byte, order binary.ByteOrder) bool {
	if len(content) < 32 {
		return false
	}
	zeroes := 0
	pairs := 0
	for index := 0; index+1 < len(content) && pairs < 512; index += 2 {
		var ascii, zero byte
		if order == binary.LittleEndian {
			ascii, zero = content[index], content[index+1]
		} else {
			zero, ascii = content[index], content[index+1]
		}
		if zero == 0 && ascii >= 9 && ascii <= 126 {
			zeroes++
		}
		pairs++
	}

	return zeroes > pairs/2
}

func decodeUTF16(content []byte, order binary.ByteOrder) string {
	units := make([]uint16, 0, len(content)/2)
	for len(content) >= 2 {
		units = append(units, order.Uint16(content[:2]))
		content = content[2:]
	}

	return strings.ToValidUTF8(string(utf16.Decode(units)), "?")
}

func importanceClaim(identity string, level int) Claim {
	return Claim{
		Subject:   identity,
		Predicate: GmeowImportanceLevel,
		Object:    termTypedInteger(level),
	}
}

func writeRecordBody(builder *strings.Builder, body string) {
	builder.WriteString("\n")
	builder.WriteString(body)
	if !strings.HasSuffix(body, "\n") {
		builder.WriteString("\n")
	}
}

// contactBundleContent assembles the whole-file bundle: prefixes once, every
// contact body, and a source import-level claim per contact. (Used for export
// round-trips and back-compat; Phase 3 ingest uses per-contact deltas.)
func contactBundleContent(
	records []renderedContact,
	contacts []string,
	level int,
) string {
	var builder strings.Builder
	writePrefixes(&builder)
	for _, record := range records {
		writeRecordBody(&builder, record.body)
	}
	claims := make([]Claim, 0, len(contacts))
	for _, contact := range uniqueStrings(contacts) {
		claims = append(claims, importanceClaim(contact, level))
	}
	builder.WriteString(BuildRDFStarDelta(claims))

	return builder.String()
}

// contactDeltaContent renders one logical contact as a standalone Turtle object:
// prefixes, the contact body, and its source import-level claim.
func contactDeltaContent(identity, body string, level int) string {
	var builder strings.Builder
	writePrefixes(&builder)
	writeRecordBody(&builder, body)
	builder.WriteString(BuildRDFStarDelta([]Claim{importanceClaim(identity, level)}))

	return builder.String()
}

func importFacets(format string, contacts []string, importLevel int) []contracts.Facet {
	input := contactentity.MetadataInput{
		Format:         MediaTypeTurtle,
		SourceKind:     contactImportSourceKind,
		ImportLevel:    importLevel,
		HasImportLevel: true,
	}
	if len(contacts) == 1 {
		input.RootSubject = contacts[0]
	}

	// One facet per object: rdf_source_bundle is load-bearing (it triggers RDF
	// statement projection in query/postgres/rdf.go and carries root_subject for
	// contact-root detection). The former contact_entity facet was redundant (only
	// a root-detection alternative; the ContactSourceRole content-role already marks
	// the object a contact), and the version_set facet was dead (no consumer). Cut to
	// reduce per-object metadata (the 4:1 metadata:content ratio from the live test).
	return []contracts.Facet{
		{
			Kind: contracts.RDFSourceBundleFacetKind,
			Metadata: contactentity.Metadata(contactentity.MetadataInput{
				RootSubject:    input.RootSubject,
				Format:         MediaTypeTurtle,
				SourceKind:     contactImportSourceKind,
				ClaimKind:      format,
				ImportLevel:    importLevel,
				HasImportLevel: true,
			}),
		},
	}
}

func nativeToRDF(content []byte) ([]renderedContact, []RecordRejection, error) {
	var bundle NativeBundle
	err := json.Unmarshal(content, &bundle)
	if err != nil {
		return nil, nil, fmt.Errorf("decode native contact bundle: %w", err)
	}

	if len(bundle.Contacts) == 0 {
		return nil, nil, errors.New("native contact bundle has no contacts")
	}

	if bundle.SchemaVersion != contracts.SchemaVersionPhase00 {
		return nil, nil, fmt.Errorf(
			"unsupported native contact schema_version %d",
			bundle.SchemaVersion,
		)
	}

	records := make([]renderedContact, 0, len(bundle.Contacts))
	for _, contact := range bundle.Contacts {
		if strings.TrimSpace(contact.ContactID) == "" {
			return nil, nil, errors.New("native contact is missing contact_id")
		}

		claims := []Claim{
			{
				Subject:   contact.ContactID,
				Predicate: rdfType,
				Object:    termIRI(foafPrefix + "Person"),
			},
		}

		if contact.DisplayName != "" {
			claims = append(claims, Claim{
				Subject:   contact.ContactID,
				Predicate: foafPrefix + "name",
				Object:    termLiteral(contact.DisplayName),
			})
		}

		if contact.PrimaryEmail != "" {
			claims = append(claims, Claim{
				Subject:   contact.ContactID,
				Predicate: schemaPrefix + "email",
				Object: termIRI(
					"mailto:" + contactentity.NormalizeIdentity(contact.PrimaryEmail),
				),
			})
		}

		for _, alias := range contact.Aliases {
			normalized := contactentity.NormalizeAlias(alias)
			if normalized == "" {
				continue
			}
			claims = append(claims, Claim{
				Subject:   contact.ContactID,
				Predicate: gmeowPrefix + "contactAlias",
				Object:    termLiteral(normalized),
			})
		}

		for _, fact := range contact.Facts {
			predicate := predicateForFact(fact)
			if predicate == "" || strings.TrimSpace(fact.Value) == "" {
				continue
			}

			object := objectForFact(fact)
			if object == "" {
				return nil, nil, fmt.Errorf(
					"native contact %q has invalid %s fact value %q",
					contact.ContactID,
					fact.FactKind,
					fact.Value,
				)
			}
			claim := Claim{
				Subject:   contact.ContactID,
				Predicate: predicate,
				Object:    termRaw(object),
			}
			if fact.ValidFrom != "" {
				claim = claim.withAnnotation(
					timePrefix+"hasBeginning",
					termTypedDate(fact.ValidFrom),
				)
			}
			if fact.ValidUntil != "" {
				claim = claim.withAnnotation(timePrefix+"hasEnd", termTypedDate(fact.ValidUntil))
			}
			claims = append(claims, claim)
		}

		records = append(
			records,
			renderedContact{identity: contact.ContactID, body: BuildRDFStarDelta(claims)},
		)
	}

	return records, nil, nil
}

func firstRDFContactSubjects(content string) []string {
	detector := rdfContactRootDetector{
		prefixes: map[string]string{
			"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		},
		subjects: map[string]bool{},
		objects:  map[string]bool{},
	}

	for line := range strings.SplitSeq(content, "\n") {
		detector.consume(line)
	}

	// Rooted single-contact graph: a declared primary subject
	// (schema:mainEntity / foaf:primaryTopic) means the ENTIRE graph is one
	// contact — embedded people/orgs/works are that contact's claims, not
	// separate contacts. Importing it as more than one contact is an error.
	if detector.primary != "" {
		return []string{detector.primary}
	}

	// Otherwise it is an un-rooted collection: each contact-typed root subject
	// is its own contact.
	roots := make([]string, 0, len(detector.subjects))
	subjects := make([]string, 0, len(detector.subjects))
	for subject := range detector.subjects {
		subjects = append(subjects, subject)
		if !detector.objects[subject] {
			roots = append(roots, subject)
		}
	}
	sort.Strings(roots)
	if len(roots) > 0 {
		return roots
	}

	sort.Strings(subjects)
	return subjects
}

type rdfContactRootDetector struct {
	currentPredicate  rdfImportTerm
	currentSubject    rdfImportTerm
	prefixes          map[string]string
	subjects          map[string]bool
	objects           map[string]bool
	primary           string // object of schema:mainEntity / foaf:primaryTopic, if declared
	continuingObjects bool
}

type rdfImportTerm struct {
	kind  string
	value string
}

func (detector *rdfContactRootDetector) consume(raw string) {
	indented := raw != "" && unicode.IsSpace(rune(raw[0]))

	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return
	}
	if parseImportRDFPrefix(line, detector.prefixes) {
		return
	}
	if indented && detector.currentSubject.value != "" {
		detector.recordIRIObjects(line)
		if !detector.consumePredicate(line) {
			detector.consumeObjectContinuation(line)
		}

		return
	}

	subject, rest, found := parseImportRDFSubjectLine(line, detector.prefixes)
	if !found {
		detector.recordIRIObjects(line)

		return
	}

	detector.currentSubject = subject
	detector.currentPredicate = rdfImportTerm{}
	detector.continuingObjects = false
	detector.recordIRIObjects(rest)
	if strings.TrimSpace(rest) != "" {
		detector.consumePredicate(rest)
	}
}

func (detector *rdfContactRootDetector) consumePredicate(line string) bool {
	predicate, statements, found := parseImportRDFPredicateObjectLine(
		detector.currentSubject,
		line,
		detector.prefixes,
	)
	if !found {
		return false
	}

	detector.currentPredicate = predicate
	detector.continuingObjects = strings.HasSuffix(line, ",")
	detector.record(statements)

	return true
}

func (detector *rdfContactRootDetector) consumeObjectContinuation(line string) {
	if detector.currentPredicate.value == "" {
		return
	}

	statements := parseImportRDFObjectList(
		detector.currentSubject,
		detector.currentPredicate,
		line,
		detector.prefixes,
	)
	detector.continuingObjects = strings.HasSuffix(line, ",")
	detector.record(statements)
}

func (detector *rdfContactRootDetector) record(statements []rdfImportStatement) {
	for _, statement := range statements {
		if statement.object.kind == "iri" {
			detector.objects[statement.object.value] = true
		}
		if statement.predicate.value == rdfType &&
			contactentity.IsContactEntityType(statement.object.value) {
			detector.subjects[statement.subject.value] = true
		}
		// A declared primary subject (schema:mainEntity / foaf:primaryTopic)
		// makes the whole graph one rooted contact — see firstRDFContactSubjects.
		if detector.primary == "" && statement.object.kind == "iri" &&
			isPrimarySubjectPredicate(statement.predicate.value) {
			detector.primary = statement.object.value
		}
	}
}

// isPrimarySubjectPredicate reports whether a predicate declares the graph's
// primary entity (the one contact a rooted single-contact graph is about).
func isPrimarySubjectPredicate(predicate string) bool {
	switch predicate {
	case schemaPrefix + "mainEntity",
		"http://schema.org/mainEntity",
		foafPrefix + "primaryTopic":
		return true
	default:
		return false
	}
}

func (detector *rdfContactRootDetector) recordIRIObjects(line string) {
	for {
		token, rest := splitImportRDFFirstToken(line)
		if token == "" {
			return
		}

		term, ok := parseImportRDFTerm(token, detector.prefixes)
		if ok && term.kind == "iri" && !contactentity.IsContactEntityType(term.value) {
			detector.objects[term.value] = true
		}

		line = rest
	}
}

type rdfImportStatement struct {
	subject   rdfImportTerm
	predicate rdfImportTerm
	object    rdfImportTerm
}

func parseImportRDFPrefix(line string, prefixes map[string]string) bool {
	if !strings.HasPrefix(line, "@prefix ") {
		return false
	}

	fields := strings.Fields(line)
	if len(fields) < 3 {
		return true
	}

	name := strings.TrimSuffix(fields[1], ":")
	value := strings.Trim(fields[2], "<>")
	if name != "" && value != "" {
		prefixes[name] = value
	}

	return true
}

func parseImportRDFSubjectLine(
	line string,
	prefixes map[string]string,
) (rdfImportTerm, string, bool) {
	first, rest := splitImportRDFFirstToken(line)
	if first == "" || !looksLikeImportRDFTerm(first) {
		return rdfImportTerm{}, "", false
	}

	subject, ok := parseImportRDFTerm(first, prefixes)
	if !ok {
		return rdfImportTerm{}, "", false
	}

	return subject, rest, true
}

func parseImportRDFPredicateObjectLine(
	subject rdfImportTerm,
	line string,
	prefixes map[string]string,
) (rdfImportTerm, []rdfImportStatement, bool) {
	predicateToken, objectText := splitImportRDFFirstToken(line)
	if predicateToken == "" || objectText == "" {
		return rdfImportTerm{}, nil, false
	}

	predicate, ok := parseImportRDFPredicateTerm(predicateToken, prefixes)
	if !ok {
		return rdfImportTerm{}, nil, false
	}

	return predicate, parseImportRDFObjectList(
		subject,
		predicate,
		objectText,
		prefixes,
	), true
}

func parseImportRDFObjectList(
	subject rdfImportTerm,
	predicate rdfImportTerm,
	line string,
	prefixes map[string]string,
) []rdfImportStatement {
	line = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(line), ";"), ".")
	statements := []rdfImportStatement{}

	for _, item := range splitImportRDFObjects(line) {
		object, ok := parseImportRDFTerm(item, prefixes)
		if !ok {
			continue
		}

		statements = append(statements, rdfImportStatement{
			subject:   subject,
			predicate: predicate,
			object:    object,
		})
	}

	return statements
}

func parseImportRDFPredicateTerm(
	token string,
	prefixes map[string]string,
) (rdfImportTerm, bool) {
	if token == "a" {
		return rdfImportTerm{kind: "iri", value: rdfType}, true
	}

	return parseImportRDFTerm(token, prefixes)
}

func parseImportRDFTerm(
	token string,
	prefixes map[string]string,
) (rdfImportTerm, bool) {
	token = cleanImportRDFToken(token)
	if token == "" || strings.HasPrefix(token, "[") {
		return rdfImportTerm{}, false
	}
	if strings.HasPrefix(token, "<") && strings.Contains(token, ">") {
		value, _, _ := strings.Cut(strings.TrimPrefix(token, "<"), ">")

		return rdfImportTerm{kind: "iri", value: value}, true
	}
	if strings.HasPrefix(token, "\"") {
		return rdfImportTerm{kind: "literal", value: token}, true
	}
	if prefix, suffix, ok := strings.Cut(token, ":"); ok {
		if base := prefixes[prefix]; base != "" {
			return rdfImportTerm{kind: "iri", value: base + suffix}, true
		}
	}

	return rdfImportTerm{}, false
}

func cleanImportRDFToken(token string) string {
	token = strings.TrimSpace(token)
	token = strings.TrimSuffix(token, ",")
	token = strings.TrimSuffix(token, ";")
	token = strings.TrimSuffix(token, ".")

	return strings.TrimSpace(token)
}

func splitImportRDFFirstToken(line string) (string, string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", ""
	}
	if strings.HasPrefix(line, "\"") {
		return splitImportRDFQuotedToken(line)
	}

	for index, char := range line {
		if unicode.IsSpace(char) {
			return line[:index], strings.TrimSpace(line[index:])
		}
	}

	return line, ""
}

func splitImportRDFQuotedToken(line string) (string, string) {
	escaped := false
	for index, char := range line[1:] {
		switch {
		case escaped:
			escaped = false
		case char == '\\':
			escaped = true
		case char == '"':
			end := index + 2
			for end < len(line) && !isASCIIWhitespace(line[end]) {
				end++
			}

			return line[:end], strings.TrimSpace(line[end:])
		}
	}

	return line, ""
}

func isASCIIWhitespace(char byte) bool {
	switch char {
	case ' ', '\t', '\n', '\r', '\v', '\f':
		return true
	default:
		return false
	}
}

func splitImportRDFObjects(line string) []string {
	items := []string{}
	start := 0
	inString := false
	escaped := false

	for index, char := range line {
		switch {
		case escaped:
			escaped = false
		case char == '\\':
			escaped = true
		case char == '"':
			inString = !inString
		case char == ',' && !inString:
			items = append(items, strings.TrimSpace(line[start:index]))
			start = index + 1
		}
	}

	items = append(items, strings.TrimSpace(line[start:]))

	return items
}

func looksLikeImportRDFTerm(token string) bool {
	return strings.HasPrefix(token, "<") ||
		strings.HasPrefix(token, "_:") ||
		strings.Contains(token, ":")
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
	contactentity.FactKindContactAlias: gmeowPrefix + "contactAlias",
	contactentity.FactKindIdentifier:   schemaPrefix + "identifier",
	contactentity.FactKindImportance:   GmeowImportanceLevel,
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
			return gmeowPrefix + "historicalEmail"
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
	case contactentity.FactKindImportance:
		level, err := parseImportLevel(value)
		if err != nil {
			return ""
		}

		return typedInteger(level)
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

func parseImportLevel(value string) (int, error) {
	var level int
	if _, err := fmt.Sscanf(strings.TrimSpace(value), "%d", &level); err != nil {
		return 0, err
	}
	if err := ValidateImportLevel(level); err != nil {
		return 0, err
	}

	return level, nil
}

func normalizedEmailIRI(value string) string {
	normalized := normalizeContactEmail(value)
	if normalized == "" {
		return ""
	}

	return iri("mailto:" + normalized)
}

func normalizeContactEmail(value string) string {
	normalized := contactentity.NormalizeIdentity(value)
	if !strings.Contains(normalized, "@") {
		return ""
	}

	return normalized
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
	builder.WriteString("@prefix gmeow: <" + gmeowPrefix + "> .\n")
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

func writeTemporalEndAnnotation(
	builder *strings.Builder,
	subject string,
	predicate string,
	object string,
	validUntil string,
) {
	if validUntil == "" {
		return
	}

	statement := "<< " + iri(subject) + " " + iri(predicate) + " " + object + " >> "
	writeStatementDateAnnotation(builder, statement, timePrefix+"hasEnd", validUntil)
}

func writeStatementDateAnnotation(
	builder *strings.Builder,
	statement string,
	predicate string,
	value string,
) {
	builder.WriteString(statement)
	builder.WriteString(iri(predicate))
	builder.WriteString(" ")
	builder.WriteString(typedDate(value))
	builder.WriteString(" .\n")
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

func typedDateTime(value time.Time) string {
	return literal(value.UTC().Format(time.RFC3339)) + "^^" + iri(xsdDateTime)
}

func typedInteger(value int) string {
	return literal(fmt.Sprintf("%d", value)) + "^^" + iri(xsdInteger)
}

// typedDateTimeStr types an already-RFC3339 string as xsd:dateTime (vs
// typedDateTime which formats a time.Time).
func typedDateTimeStr(value string) string {
	return literal(value) + "^^" + iri(xsdDateTime)
}

// typedDecimal types a decimal literal (e.g. a confidence in [0,1]).
func typedDecimal(value string) string {
	return literal(value) + "^^" + iri(xsdDecimal)
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

// ContentDigest is the stable content identity of a source artifact for the
// gmeow:Source node (four-clock model): two imports of the same bytes share it,
// regardless of path or mtime.
func ContentDigest(content []byte) string {
	return "sha256:" + contentHash(content)
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
