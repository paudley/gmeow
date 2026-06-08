// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"errors"
	"fmt"
	"strings"
)

func bbdbToRDF(content string) ([]renderedContact, []RecordRejection, error) {
	parsed, err := parseBBDBContacts(content)
	if err != nil {
		return nil, nil, err
	}
	if len(parsed) == 0 {
		return nil, nil, errors.New("BBDB import has no contacts")
	}

	records := make([]renderedContact, 0, len(parsed))
	for _, contact := range parsed {
		var body strings.Builder
		writeBBDBContactRDF(&body, contact)
		records = append(
			records,
			renderedContact{identity: contact.subject, body: body.String()},
		)
	}

	return records, nil, nil
}

func writeBBDBContactRDF(builder *strings.Builder, contact bbdbContact) {
	builder.WriteString("\n")
	writeTriple(builder, contact.subject, rdfType, iri(foafPrefix+"Person"))
	if contact.displayName != "" {
		writeTriple(builder, contact.subject, foafPrefix+"name", literal(contact.displayName))
	}
	for _, alias := range contact.aliases {
		if alias = strings.TrimSpace(alias); alias != "" {
			writeTriple(builder, contact.subject, foafPrefix+"nick", literal(alias))
		}
	}
	for _, email := range contact.emails {
		if object := normalizedEmailIRI(email); object != "" {
			writeTriple(builder, contact.subject, vcardPrefix+"hasEmail", object)
		}
	}
	for index, phone := range contact.phones {
		writeBBDBPhoneRDF(builder, contact.subject, index, phone)
	}
	for index, address := range contact.addresses {
		writeBBDBAddressRDF(builder, contact.subject, index, address)
	}
	if contact.company != "" {
		writeTriple(
			builder,
			contact.subject,
			schemaPrefix+"affiliation",
			literal(contact.company),
		)
	}
	for _, field := range contact.userFields {
		predicate := bbdbUserFieldPredicates[field.name]
		if predicate == "" {
			continue
		}
		object := literal(field.value)
		writeTriple(builder, contact.subject, predicate, object)
		if lifecycle, found := providerLifecycleByPredicate[predicate]; found {
			writeTemporalEndAnnotation(
				builder,
				contact.subject,
				predicate,
				object,
				lifecycle.ValidUntil,
			)
		}
	}
}

func writeBBDBPhoneRDF(
	builder *strings.Builder,
	contact string,
	index int,
	phone bbdbPhone,
) {
	subject := structuredBBDBNode(contact, "phone", index)
	writeTriple(builder, contact, gmeowPrefix+"bbdbPhone", iri(subject))
	writeTriple(builder, subject, rdfType, iri(gmeowPrefix+"BBDBPhone"))
	writeOptionalLiteralTriple(builder, subject, gmeowPrefix+"bbdbPhoneLabel", phone.Label)
	writeOptionalLiteralTriple(builder, subject, schemaPrefix+"telephone", phone.Number)
	writeOptionalLiteralTriple(builder, subject, gmeowPrefix+"bbdbPhoneArea", phone.Area)
	writeOptionalLiteralTriple(
		builder,
		subject,
		gmeowPrefix+"bbdbPhoneExchange",
		phone.Exchange,
	)
	writeOptionalLiteralTriple(
		builder,
		subject,
		gmeowPrefix+"bbdbPhoneSubscriber",
		phone.Subscriber,
	)
	writeOptionalLiteralTriple(
		builder,
		subject,
		gmeowPrefix+"bbdbPhoneExtension",
		phone.Extension,
	)
}

func writeBBDBAddressRDF(
	builder *strings.Builder,
	contact string,
	index int,
	address bbdbAddress,
) {
	subject := structuredBBDBNode(contact, "address", index)
	writeTriple(builder, contact, gmeowPrefix+"bbdbAddress", iri(subject))
	writeTriple(builder, subject, rdfType, iri(gmeowPrefix+"BBDBAddress"))
	writeOptionalLiteralTriple(
		builder,
		subject,
		gmeowPrefix+"bbdbAddressLabel",
		address.Label,
	)
	for _, street := range address.Streets {
		writeOptionalLiteralTriple(builder, subject, schemaPrefix+"streetAddress", street)
	}
	writeOptionalLiteralTriple(
		builder,
		subject,
		schemaPrefix+"addressLocality",
		address.City,
	)
	writeOptionalLiteralTriple(
		builder,
		subject,
		schemaPrefix+"addressRegion",
		address.State,
	)
	writeOptionalLiteralTriple(
		builder,
		subject,
		schemaPrefix+"postalCode",
		address.Postcode,
	)
	writeOptionalLiteralTriple(
		builder,
		subject,
		schemaPrefix+"addressCountry",
		address.Country,
	)
}

func writeOptionalLiteralTriple(
	builder *strings.Builder,
	subject, predicate, value string,
) {
	if value = strings.TrimSpace(value); value != "" {
		writeTriple(builder, subject, predicate, literal(value))
	}
}

func parseBBDBContacts(content string) ([]bbdbContact, error) {
	contacts := []bbdbContact{}
	var record strings.Builder
	recordStart := 0
	depth := 0
	for lineNumber, raw := range strings.Split(content, "\n") {
		line := strings.TrimSpace(raw)
		if line == "" || strings.HasPrefix(line, ";") {
			continue
		}
		if !strings.HasPrefix(line, "[") {
			if depth == 0 {
				continue
			}
		}
		if depth == 0 {
			record.Reset()
			recordStart = lineNumber + 1
		} else {
			record.WriteByte(' ')
		}
		record.WriteString(line)
		depth += bbdbListDepthDelta(line)
		if depth > 0 {
			continue
		}

		parser := bbdbParser{input: record.String()}
		value, err := parser.parseValue()
		if err != nil {
			return nil, fmt.Errorf("parse BBDB line %d: %w", recordStart, err)
		}
		record, ok := bbdbContactFromValue(value)
		if !ok {
			return nil, fmt.Errorf("unsupported BBDB contact record on line %d", recordStart)
		}
		contacts = append(contacts, record)
	}
	if depth != 0 {
		return nil, errors.New("unterminated BBDB contact record")
	}

	return contacts, nil
}

func bbdbListDepthDelta(line string) int {
	depth := 0
	inString := false
	escaped := false
	for _, char := range line {
		if escaped {
			escaped = false
			continue
		}
		if inString && char == '\\' {
			escaped = true
			continue
		}
		if char == '"' {
			inString = !inString
			continue
		}
		if inString {
			continue
		}
		switch char {
		case '[', '(':
			depth++
		case ']', ')':
			depth--
		}
	}

	return depth
}

func bbdbContactFromValue(value bbdbValue) (bbdbContact, bool) {
	fields := value.items
	if len(fields) < 7 {
		return bbdbContact{}, false
	}

	first := fields[0].stringValue()
	last := fields[1].stringValue()
	aliases := bbdbStringList(fields[2])
	company := fields[3].stringValue()
	phones, ok := bbdbPhones(fields[4])
	if !ok {
		return bbdbContact{}, false
	}
	addresses, ok := bbdbAddresses(fields[5])
	if !ok {
		return bbdbContact{}, false
	}
	emails := bbdbStringList(fields[6])
	userFields := []bbdbUserField{}
	if len(fields) > 7 {
		userFields, ok = bbdbUserFields(fields[7])
		if !ok {
			return bbdbContact{}, false
		}
	}

	displayName := strings.TrimSpace(strings.Join([]string{first, last}, " "))
	if displayName == "" {
		displayName = firstNonEmpty(aliases...)
	}
	if displayName == "" {
		displayName = company
	}

	contact := bbdbContact{
		addresses:   addresses,
		aliases:     aliases,
		company:     company,
		displayName: displayName,
		emails:      emails,
		phones:      phones,
		userFields:  userFields,
		subject:     contactSubjectForBBDB(displayName, company, aliases, emails),
	}
	if contact.subject == "" {
		return bbdbContact{}, false
	}

	return contact, true
}

func bbdbUserFields(value bbdbValue) ([]bbdbUserField, bool) {
	if value.symbol == "nil" || len(value.items) == 0 {
		return nil, true
	}

	fields := []bbdbUserField{}
	for _, item := range value.items {
		if len(item.items) != 3 ||
			item.items[1].symbol != "." ||
			item.items[0].symbol == "" {
			return nil, false
		}
		name := item.items[0].symbol
		predicate := bbdbUserFieldPredicates[name]
		if predicate == "" {
			return nil, false
		}
		fieldValue := item.items[2].stringValue()
		if fieldValue == "" && item.items[2].symbol != "nil" {
			fieldValue = item.items[2].symbol
		}
		if fieldValue != "" {
			fields = append(fields, bbdbUserField{name: name, value: fieldValue})
		}
	}

	return fields, true
}

func bbdbPhones(value bbdbValue) ([]bbdbPhone, bool) {
	if value.symbol == "nil" || len(value.items) == 0 {
		return nil, true
	}

	phones := []bbdbPhone{}
	for _, item := range value.items {
		if len(item.items) != 2 && len(item.items) != 5 {
			return nil, false
		}
		phone := bbdbPhone{
			Label: item.items[0].stringValue(),
		}
		if len(item.items) == 2 {
			phone.Number = item.items[1].stringValue()
		} else {
			phone.Area = item.items[1].scalarText()
			phone.Exchange = item.items[2].scalarText()
			phone.Subscriber = item.items[3].scalarText()
			phone.Extension = item.items[4].scalarText()
		}
		if phone.Label != "" ||
			phone.Number != "" ||
			phone.Area != "" ||
			phone.Exchange != "" ||
			phone.Subscriber != "" ||
			phone.Extension != "" {
			phones = append(phones, phone)
		}
	}

	return phones, true
}

func bbdbAddresses(value bbdbValue) ([]bbdbAddress, bool) {
	if value.symbol == "nil" || len(value.items) == 0 {
		return nil, true
	}

	addresses := []bbdbAddress{}
	for _, item := range value.items {
		if len(item.items) != 6 {
			return nil, false
		}
		address := bbdbAddress{
			Label:    item.items[0].stringValue(),
			Streets:  bbdbStringList(item.items[1]),
			City:     item.items[2].stringValue(),
			State:    item.items[3].stringValue(),
			Postcode: item.items[4].stringValue(),
			Country:  item.items[5].stringValue(),
		}
		if address.Label != "" ||
			len(address.Streets) > 0 ||
			address.City != "" ||
			address.State != "" ||
			address.Postcode != "" ||
			address.Country != "" {
			addresses = append(addresses, address)
		}
	}

	return addresses, true
}

var bbdbUserFieldPredicates = map[string]string{
	"aim":              gmeowPrefix + "aimIdentity",
	"attribution":      gmeowPrefix + "bbdbAttribution",
	"calllog-1":        gmeowPrefix + "bbdbCallLog",
	"country":          schemaPrefix + "nationality",
	"creation-date":    gmeowPrefix + "bbdbCreationDate",
	"get-pgp-key":      gmeowPrefix + "bbdbGetPGPKey",
	"gtalk":            gmeowPrefix + "googleTalkIdentity",
	"icq":              gmeowPrefix + "icqIdentity",
	"jabber":           gmeowPrefix + "jabberIdentity",
	"last-help":        gmeowPrefix + "bbdbListHelp",
	"last-list":        gmeowPrefix + "bbdbListList",
	"last-noticed":     gmeowPrefix + "bbdbLastNoticed",
	"last-subject":     gmeowPrefix + "bbdbLastSubject",
	"last-url":         schemaPrefix + "url",
	"list-address":     gmeowPrefix + "bbdbListAddress",
	"list-help":        gmeowPrefix + "bbdbListHelp",
	"list-list":        gmeowPrefix + "bbdbListList",
	"list-subscribe":   gmeowPrefix + "bbdbListSubscribe",
	"list-unsubscribe": gmeowPrefix + "bbdbListUnsubscribe",
	"lists-offered":    gmeowPrefix + "bbdbListsOffered",
	"mailinglists":     gmeowPrefix + "bbdbMailingLists",
	"mailingslists":    gmeowPrefix + "bbdbMailingLists",
	"msn":              gmeowPrefix + "msnIdentity",
	"nic":              gmeowPrefix + "bbdbNIC",
	"nic-organization": gmeowPrefix + "bbdbNICOrganization",
	"nic-updated":      gmeowPrefix + "bbdbNICUpdated",
	"newsgroups":       gmeowPrefix + "bbdbNewsgroups",
	"notes":            gmeowPrefix + "bbdbNotes",
	"parent":           gmeowPrefix + "bbdbParent",
	"password":         gmeowPrefix + "bbdbPassword",
	"pgp-finger-print": gmeowPrefix + "bbdbPGPFingerprint",
	"precedence":       gmeowPrefix + "bbdbPrecedence",
	"skype":            gmeowPrefix + "skypeIdentity",
	"status":           gmeowPrefix + "status",
	"timestamp":        gmeowPrefix + "bbdbTimestamp",
	"url":              schemaPrefix + "url",
	"www":              schemaPrefix + "url",
	"xface":            gmeowPrefix + "xFace",
	"yahoo":            gmeowPrefix + "yahooIdentity",
}

func contactSubjectForBBDB(
	displayName string,
	company string,
	aliases []string,
	emails []string,
) string {
	for _, email := range emails {
		normalized := normalizeContactEmail(email)
		if normalized != "" {
			return "mailto:" + normalized
		}
	}

	key := firstNonEmpty(displayName, company, strings.Join(aliases, "|"))
	if key == "" {
		return ""
	}

	return "urn:gmeow:contact:" + shortHash([]byte("bbdb:"+key))
}

func structuredBBDBNode(contact, kind string, index int) string {
	return "urn:gmeow:contact:bbdb:" + kind + ":" +
		shortHash([]byte(fmt.Sprintf("%s:%s:%d", contact, kind, index)))
}

func bbdbStringList(value bbdbValue) []string {
	values := []string{}
	for _, item := range value.items {
		if text := item.stringValue(); text != "" {
			values = append(values, text)
		}
	}

	return values
}

func (value bbdbValue) stringValue() string {
	if !value.quoted {
		return ""
	}

	return strings.TrimSpace(value.text)
}

func (value bbdbValue) scalarText() string {
	if value.quoted {
		return strings.TrimSpace(value.text)
	}
	if value.symbol == "nil" {
		return ""
	}

	return strings.TrimSpace(value.symbol)
}

func (parser *bbdbParser) parseValue() (bbdbValue, error) {
	parser.skipSpace()
	if parser.index >= len(parser.input) {
		return bbdbValue{}, errors.New("unexpected end of input")
	}

	switch parser.input[parser.index] {
	case '[', '(':
		return parser.parseList()
	case '"':
		return parser.parseString()
	default:
		return parser.parseSymbol()
	}
}

func (parser *bbdbParser) parseList() (bbdbValue, error) {
	open := parser.input[parser.index]
	close := byte(')')
	if open == '[' {
		close = ']'
	}
	parser.index++

	items := []bbdbValue{}
	for {
		parser.skipSpace()
		if parser.index >= len(parser.input) {
			return bbdbValue{}, errors.New("unterminated list")
		}
		if parser.input[parser.index] == close {
			parser.index++

			return bbdbValue{items: items}, nil
		}

		item, err := parser.parseValue()
		if err != nil {
			return bbdbValue{}, err
		}
		items = append(items, item)
	}
}

func (parser *bbdbParser) parseString() (bbdbValue, error) {
	parser.index++
	var builder strings.Builder
	for parser.index < len(parser.input) {
		char := parser.input[parser.index]
		parser.index++
		if char == '"' {
			return bbdbValue{text: builder.String(), quoted: true}, nil
		}
		if char == '\\' && parser.index < len(parser.input) {
			next := parser.input[parser.index]
			parser.index++
			switch next {
			case 'n':
				builder.WriteByte('\n')
			case 't':
				builder.WriteByte('\t')
			default:
				builder.WriteByte(next)
			}
			continue
		}

		builder.WriteByte(char)
	}

	return bbdbValue{}, errors.New("unterminated string")
}

func (parser *bbdbParser) parseSymbol() (bbdbValue, error) {
	start := parser.index
	for parser.index < len(parser.input) {
		char := parser.input[parser.index]
		if char == '[' || char == ']' || char == '(' || char == ')' ||
			char == '"' || char == ' ' || char == '\t' {
			break
		}
		parser.index++
	}
	if start == parser.index {
		return bbdbValue{}, fmt.Errorf("unexpected character %q", parser.input[parser.index])
	}

	return bbdbValue{symbol: parser.input[start:parser.index]}, nil
}

func (parser *bbdbParser) skipSpace() {
	for parser.index < len(parser.input) {
		switch parser.input[parser.index] {
		case ' ', '\t', '\r', '\n':
			parser.index++
		default:
			return
		}
	}
}
