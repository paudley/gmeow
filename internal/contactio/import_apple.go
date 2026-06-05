// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"errors"
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/micromdm/plist"

	"blackcat.ca/gmeow/internal/ontology"
)

func appleAddressBookPersonToRDF(
	content []byte,
) ([]renderedContact, []RecordRejection, error) {
	var record map[string]any
	if err := plist.Unmarshal(content, &record); err != nil {
		return nil, nil, fmt.Errorf("parse Apple AddressBook plist: %w", err)
	}
	if len(record) == 0 {
		return nil, nil, errors.New("Apple AddressBook import is empty")
	}
	if _, found := record["UID"].(string); !found {
		return nil, nil, errors.New("Apple AddressBook person import has no UID")
	}

	subject := appleAddressBookSubject(record, content)
	var body strings.Builder
	writeTriple(&body, subject, rdfType, iri(foafPrefix+"Person"))

	if err := writeAppleAddressBookScalars(&body, subject, record); err != nil {
		return nil, nil, err
	}
	if err := writeAppleAddressBookMultiValues(&body, subject, record); err != nil {
		return nil, nil, err
	}
	if err := writeAppleAddressBookPropertyTypes(&body, subject, record); err != nil {
		return nil, nil, err
	}

	return []renderedContact{{identity: subject, body: body.String()}}, nil, nil
}

// appleAddressBookGroupToRDF maps an Apple AddressBook ABGroup (.abcdg) record:
// a foaf:Group with its name, membership edges to member persons, and the
// per-member distribution-list selections. Every field is mapped or the record
// is rejected (no opaque bucket).
func appleAddressBookGroupToRDF(
	content []byte,
) ([]renderedContact, []RecordRejection, error) {
	var record map[string]any
	if err := plist.Unmarshal(content, &record); err != nil {
		return nil, nil, fmt.Errorf("parse Apple AddressBook group plist: %w", err)
	}
	uid := appleString(record["UID"])
	if uid == "" {
		return nil, nil, errors.New("Apple AddressBook group import has no UID")
	}
	// Smart groups are saved searches (a SearchElement query tree), not a
	// contact-domain group of agents — out of scope, like .abcds saved searches.
	if appleString(record["ABGroupClassKey"]) == "ABSmartGroup" ||
		record["SearchElement"] != nil {
		return nil, nil, ErrSkipNonContactDomain
	}
	subject := "urn:gmeow:contact:apple-addressbook:" + encodeIRI(uid)

	claims := []Claim{
		{Subject: subject, Predicate: rdfType, Object: termIRI(foafPrefix + "Group")},
		{Subject: subject, Predicate: schemaPrefix + "identifier", Object: termLiteral(uid)},
	}
	for _, key := range sortedMapKeys(record) {
		switch key {
		case "UID":
			// handled above
		case "GroupName":
			if name := appleString(record[key]); name != "" {
				claims = append(
					claims,
					Claim{
						Subject:   subject,
						Predicate: schemaPrefix + "name",
						Object:    termLiteral(name),
					},
				)
			}
		case "ABGroupClassKey":
			if class := appleString(record[key]); class != "" {
				claims = append(
					claims,
					Claim{
						Subject:   subject,
						Predicate: gmeowPrefix + "appleGroupClass",
						Object:    termLiteral(class),
					},
				)
			}
		case "ABMembers":
			for _, member := range appleStringList(record[key]) {
				memberSubject := "urn:gmeow:contact:apple-addressbook:" + encodeIRI(member)
				claims = append(
					claims,
					Claim{
						Subject:   subject,
						Predicate: foafPrefix + "member",
						Object:    termIRI(memberSubject),
					},
				)
			}
		case "ABEmailDistributionList",
			"ABAddressDistributionList",
			"ABPhoneDistributionList":
			dist, ok := record[key].(map[string]any)
			if !ok {
				return nil, nil, fmt.Errorf(
					"Apple AddressBook group field %q has type %T",
					key,
					record[key],
				)
			}
			predicate := gmeowPrefix + "apple" + strings.TrimPrefix(key, "AB")
			for _, member := range sortedMapKeys(dist) {
				claims = append(claims, Claim{
					Subject:   subject,
					Predicate: predicate,
					Object:    termLiteral(member + "=" + appleString(dist[member])),
				})
			}
		default:
			return nil, nil, fmt.Errorf("unsupported Apple AddressBook group field %q", key)
		}
	}

	return []renderedContact{
		{identity: subject, body: BuildRDFStarDelta(claims)},
	}, nil, nil
}

func appleAddressBookSubject(record map[string]any, content []byte) string {
	for _, value := range appleMultiValueStrings(record["Email"]) {
		if normalized := normalizeContactEmail(value); normalized != "" {
			return "mailto:" + normalized
		}
	}
	if uid := appleString(record["UID"]); uid != "" {
		return "urn:gmeow:contact:apple-addressbook:" + encodeIRI(uid)
	}

	return "urn:gmeow:contact:apple-addressbook:" + shortHash(content)
}

func writeAppleAddressBookScalars(
	builder *strings.Builder,
	subject string,
	record map[string]any,
) error {
	var name nameInput
	for key, value := range record {
		if key == "ABPropertyTypes" {
			continue
		}
		mapping, found := appleAddressBookScalarFields[key]
		if !found {
			if _, isMulti := appleAddressBookMultiValueFields[key]; isMulti {
				continue
			}
			return fmt.Errorf("unsupported Apple AddressBook person field %q", key)
		}
		object, err := appleObjectForValue(value, mapping.Kind)
		if err != nil {
			return fmt.Errorf("Apple AddressBook field %q: %w", key, err)
		}
		if object == "" {
			continue
		}
		// Name fields are reified onto one gmeow:PersonName node after the loop.
		if field, ok := nameFieldForPredicate(mapping.Predicate); ok {
			if raw := strings.TrimSpace(fmt.Sprintf("%v", value)); raw != "" {
				assignNameField(&name, field, raw)
			}

			continue
		}
		writeTriple(builder, subject, mapping.Predicate, object)
	}
	writeNameNode(builder, subject, name)

	return nil
}

func writeAppleAddressBookMultiValues(
	builder *strings.Builder,
	subject string,
	record map[string]any,
) error {
	for key, mapping := range appleAddressBookMultiValueFields {
		raw, found := record[key]
		if !found {
			continue
		}
		values, err := appleMultiValue(raw)
		if err != nil {
			return fmt.Errorf("Apple AddressBook field %q: %w", key, err)
		}
		for index, entry := range values {
			node := appleSubresource(subject, key, index)
			writeTriple(builder, subject, mapping.LinkPredicate, iri(node))
			if entry.Identifier != "" {
				writeTriple(builder, node, gmeowPrefix+"appleIdentifier", literal(entry.Identifier))
			}
			if entry.Label != "" {
				writeTriple(builder, node, gmeowPrefix+"appleLabel", literal(entry.Label))
			}
			if entry.Primary {
				writeTriple(builder, node, gmeowPrefix+"applePrimary", literal("true"))
			}
			if len(entry.Components) > 0 {
				for _, component := range sortedMapKeys(entry.Components) {
					predicate := mapping.ComponentPredicate[component]
					if predicate == "" {
						return fmt.Errorf(
							"unsupported Apple AddressBook component %q in field %q",
							component,
							key,
						)
					}
					object, err := appleObjectForValue(entry.Components[component], "")
					if err != nil {
						return fmt.Errorf(
							"Apple AddressBook component %q in field %q: %w",
							component,
							key,
							err,
						)
					}
					if object != "" {
						writeTriple(builder, node, predicate, object)
					}
				}
				continue
			}
			object, err := appleObjectForValue(entry.Value, mapping.ValueKind)
			if err != nil {
				return fmt.Errorf("Apple AddressBook field %q value: %w", key, err)
			}
			if object == "" {
				continue
			}
			writeTriple(builder, node, mapping.ValuePredicate, object)
			writeTriple(builder, subject, mapping.ValuePredicate, object)
			writeProviderLifecycle(builder, subject, mapping.ValuePredicate, object)
		}
	}

	return nil
}

func writeAppleAddressBookPropertyTypes(
	builder *strings.Builder,
	subject string,
	record map[string]any,
) error {
	raw, found := record["ABPropertyTypes"]
	if !found {
		return nil
	}
	propertyTypes, ok := raw.(map[string]any)
	if !ok {
		return fmt.Errorf("Apple AddressBook ABPropertyTypes has type %T", raw)
	}
	for _, name := range sortedMapKeys(propertyTypes) {
		if !knownAppleAddressBookPropertyType(name) {
			return fmt.Errorf("unsupported Apple AddressBook property type %q", name)
		}
		code, err := appleInteger(propertyTypes[name])
		if err != nil {
			return fmt.Errorf("Apple AddressBook property type %q: %w", name, err)
		}
		node := appleSubresource(subject, "ABPropertyTypes-"+name, 0)
		writeTriple(builder, subject, gmeowPrefix+"applePropertyType", iri(node))
		writeTriple(builder, node, gmeowPrefix+"applePropertyTypeName", literal(name))
		writeTriple(builder, node, gmeowPrefix+"applePropertyTypeCode", typedInteger(code))
	}

	return nil
}

func writeProviderLifecycle(
	builder *strings.Builder,
	subject, predicate, object string,
) {
	if lifecycle, found := providerLifecycleByPredicate[predicate]; found {
		writeTemporalEndAnnotation(
			builder,
			subject,
			predicate,
			object,
			lifecycle.ValidUntil,
		)
	}
}

type appleMultiEntry struct {
	Identifier string
	Label      string
	Primary    bool
	Value      any
	Components map[string]any
}

func appleMultiValue(raw any) ([]appleMultiEntry, error) {
	data, ok := raw.(map[string]any)
	if !ok {
		return nil, fmt.Errorf("multi-value has type %T", raw)
	}
	for key := range data {
		switch key {
		case "identifiers", "labels", "primary", "values":
		default:
			return nil, fmt.Errorf("unsupported multi-value key %q", key)
		}
	}

	values, err := appleList(data["values"])
	if err != nil {
		return nil, fmt.Errorf("values: %w", err)
	}
	labels, err := appleOptionalStringList(data["labels"])
	if err != nil {
		return nil, fmt.Errorf("labels: %w", err)
	}
	identifiers, err := appleOptionalStringList(data["identifiers"])
	if err != nil {
		return nil, fmt.Errorf("identifiers: %w", err)
	}
	primary := appleString(data["primary"])

	entries := make([]appleMultiEntry, 0, len(values))
	for index, value := range values {
		entry := appleMultiEntry{Value: value}
		if index < len(labels) {
			entry.Label = labels[index]
		}
		if index < len(identifiers) {
			entry.Identifier = identifiers[index]
			entry.Primary = entry.Identifier != "" && entry.Identifier == primary
		}
		if components, ok := value.(map[string]any); ok {
			entry.Components = components
			entry.Value = nil
		}
		entries = append(entries, entry)
	}

	return entries, nil
}

// appleStringList extracts a plain list of strings (e.g. ABGroup ABMembers),
// as distinct from the {identifiers,labels,primary,values} multi-value shape.
func appleStringList(raw any) []string {
	switch typed := raw.(type) {
	case []any:
		out := make([]string, 0, len(typed))
		for _, item := range typed {
			if value := appleString(item); value != "" {
				out = append(out, value)
			}
		}

		return out
	case string:
		if typed != "" {
			return []string{typed}
		}
	}

	return nil
}

func appleMultiValueStrings(raw any) []string {
	entries, err := appleMultiValue(raw)
	if err != nil {
		return nil
	}
	values := []string{}
	for _, entry := range entries {
		if value := appleString(entry.Value); value != "" {
			values = append(values, value)
		}
	}

	return values
}

func appleObjectForValue(value any, kind string) (string, error) {
	switch kind {
	case "email":
		text := appleString(value)
		if text == "" {
			return "", nil
		}
		if object := normalizedEmailIRI(text); object != "" {
			return object, nil
		}
		return literal(text), nil
	case "iri":
		text := appleString(value)
		if text == "" {
			return "", nil
		}
		if strings.Contains(text, ":") {
			return iri(text), nil
		}
		return literal(text), nil
	case "integer":
		integer, err := appleInteger(value)
		if err != nil {
			return "", err
		}
		return typedInteger(integer), nil
	case "float":
		number, err := appleFloat(value)
		if err != nil {
			return "", err
		}
		return literal(fmt.Sprintf("%g", number)), nil
	case "datetime":
		dateTime, ok := value.(time.Time)
		if !ok {
			return "", fmt.Errorf("expected datetime, got %T", value)
		}
		return typedDateTime(dateTime), nil
	default:
		text := appleString(value)
		if text == "" {
			return "", nil
		}
		return literal(text), nil
	}
}

func appleList(value any) ([]any, error) {
	if value == nil {
		return nil, nil
	}
	list, ok := value.([]any)
	if !ok {
		return nil, fmt.Errorf("expected list, got %T", value)
	}

	return list, nil
}

func appleOptionalStringList(value any) ([]string, error) {
	list, err := appleList(value)
	if err != nil {
		return nil, err
	}
	values := make([]string, 0, len(list))
	for _, item := range list {
		text := appleString(item)
		if text == "" {
			return nil, fmt.Errorf("expected non-empty string list item, got %T", item)
		}
		values = append(values, text)
	}

	return values, nil
}

func appleString(value any) string {
	if typed, ok := value.(string); ok {
		return strings.TrimSpace(typed)
	}

	return ""
}

func appleInteger(value any) (int, error) {
	switch typed := value.(type) {
	case int:
		return typed, nil
	case int64:
		return int(typed), nil
	case uint64:
		return int(typed), nil
	default:
		return 0, fmt.Errorf("expected integer, got %T", value)
	}
}

func appleFloat(value any) (float64, error) {
	switch typed := value.(type) {
	case float32:
		return float64(typed), nil
	case float64:
		return typed, nil
	case int64:
		return float64(typed), nil
	case uint64:
		return float64(typed), nil
	default:
		return 0, fmt.Errorf("expected number, got %T", value)
	}
}

func appleSubresource(subject, field string, index int) string {
	return subject + "#apple-" + compactPredicateName(
		field,
	) + "-" + fmt.Sprintf(
		"%d",
		index,
	)
}

func sortedMapKeys(values map[string]any) []string {
	keys := make([]string, 0, len(values))
	for key := range values {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

var appleAddressBookScalarFields = map[string]appleScalarField{
	"ABDepartment": {Predicate: gmeowPrefix + "appleDepartment"},
	"ABPersonFlags": {
		Predicate: gmeowPrefix + "applePersonFlags",
		Kind:      "integer",
	},
	"Birthday": {
		Predicate: schemaPrefix + "birthDate",
		Kind:      "datetime",
	},
	"Creation": {
		Predicate: gmeowPrefix + "appleCreationTime",
		Kind:      "datetime",
	},
	"First":      {Predicate: schemaPrefix + "givenName"},
	"JobTitle":   {Predicate: schemaPrefix + "jobTitle"},
	"Last":       {Predicate: schemaPrefix + "familyName"},
	"MaidenName": {Predicate: gmeowPrefix + "maidenName"},
	"Middle":     {Predicate: schemaPrefix + "additionalName"},
	"Modification": {
		Predicate: gmeowPrefix + "appleModificationTime",
		Kind:      "datetime",
	},
	"Nickname":     {Predicate: schemaPrefix + "alternateName"},
	"Note":         {Predicate: schemaPrefix + "description"},
	"Organization": {Predicate: schemaPrefix + "affiliation"},
	"PlaxoMember":  {Predicate: gmeowPrefix + "plaxoMember"},
	"PlaxoState":   {Predicate: gmeowPrefix + "plaxoState"},
	"Suffix":       {Predicate: schemaPrefix + "honorificSuffix"},
	"Title":        {Predicate: schemaPrefix + "honorificPrefix"},
	"UID":          {Predicate: schemaPrefix + "identifier"},
	"com.postbox-inc.popularityIndex": {
		Predicate: gmeowPrefix + "postboxPopularityIndex",
		Kind:      "float",
	},
}

var appleAddressBookMultiValueFields = map[string]appleMultiValueField{
	"ABDate": {
		LinkPredicate:  gmeowPrefix + "appleDateEntry",
		ValuePredicate: gmeowPrefix + "appleDate",
		ValueKind:      "datetime",
	},
	"ABRelatedNames": {
		LinkPredicate:  gmeowPrefix + "appleRelatedNameEntry",
		ValuePredicate: schemaPrefix + "relatedTo",
	},
	"AIMInstant": {
		LinkPredicate:  gmeowPrefix + "appleAIMInstantEntry",
		ValuePredicate: gmeowPrefix + "aimIdentity",
	},
	"Email": {
		LinkPredicate:  gmeowPrefix + "appleEmailEntry",
		ValuePredicate: schemaPrefix + "email",
		ValueKind:      "email",
	},
	"GoogleInstant": {
		LinkPredicate:  gmeowPrefix + "appleGoogleInstantEntry",
		ValuePredicate: gmeowPrefix + "googleTalkIdentity",
	},
	"ICQInstant": {
		LinkPredicate:  gmeowPrefix + "appleICQInstantEntry",
		ValuePredicate: gmeowPrefix + "icqIdentity",
	},
	"InstantMessage": {
		LinkPredicate: gmeowPrefix + "appleInstantMessageEntry",
		ComponentPredicate: map[string]string{
			"InstantMessageService":  gmeowPrefix + "instantMessageService",
			"InstantMessageUsername": gmeowPrefix + "instantMessageIdentity",
		},
	},
	"JabberInstant": {
		LinkPredicate:  gmeowPrefix + "appleJabberInstantEntry",
		ValuePredicate: gmeowPrefix + "jabberIdentity",
	},
	"MSNInstant": {
		LinkPredicate:  gmeowPrefix + "appleMSNInstantEntry",
		ValuePredicate: gmeowPrefix + "msnIdentity",
	},
	"OtherInstant": {
		LinkPredicate:  gmeowPrefix + "appleOtherInstantEntry",
		ValuePredicate: gmeowPrefix + "otherInstantIdentity",
	},
	"Phone": {
		LinkPredicate:  gmeowPrefix + "applePhoneEntry",
		ValuePredicate: schemaPrefix + "telephone",
	},
	"PlaxoMicroBlog": {
		LinkPredicate:  gmeowPrefix + "applePlaxoMicroBlogEntry",
		ValuePredicate: gmeowPrefix + "plaxoMicroBlog",
	},
	"SkypeInstant": {
		LinkPredicate:  gmeowPrefix + "appleSkypeInstantEntry",
		ValuePredicate: gmeowPrefix + "skypeIdentity",
	},
	"URLs": {
		LinkPredicate:  gmeowPrefix + "appleURLEntry",
		ValuePredicate: schemaPrefix + "url",
		ValueKind:      "iri",
	},
	"YahooInstant": {
		LinkPredicate:  gmeowPrefix + "appleYahooInstantEntry",
		ValuePredicate: gmeowPrefix + "yahooIdentity",
	},
	"Address": {
		LinkPredicate: gmeowPrefix + "appleAddressEntry",
		ComponentPredicate: map[string]string{
			"City":        ontology.AddressLocality,
			"Country":     ontology.CountryCode,
			"CountryCode": ontology.CountryCode,
			"State":       ontology.AddressRegion,
			"Street":      ontology.StreetAddress,
			"ZIP":         ontology.PostalCode,
		},
	},
}

var appleAddressBookPropertyTypes = stringSet([]string{
	"ABDate", "ABDateComponents", "ABDepartment", "ABPersonFlags", "ABRelatedNames",
	"AIMInstant", "Address", "Birthday", "BirthdayComponents", "City", "Country",
	"CountryCode", "Creation", "Email", "First", "FirstPhonetic", "GCKAPEProperty",
	"GCKBankNumberProperty", "GCKCategoryProperty", "GCKCustomerNumberProperty",
	"GCKDivisionProperty", "GCKEmployeesProperty", "GCKIndutsryProperty",
	"GCKRegionProperty", "GCKSicProperty", "GCKStockProperty", "GCKTerritortyProperty",
	"GCKVATNumberProperty", "GoogleInstant", "HomePage", "ICQInstant", "InstantMessage",
	"JabberInstant", "JobTitle", "Last", "LastPhonetic", "MSNInstant", "MaidenName",
	"Middle", "MiddlePhonetic", "Modification", "Nickname", "Note", "OUN2", "OUN3",
	"Organization", "OtherInstant", "Phone", "PlaxoMember", "PlaxoMicroBlog",
	"PlaxoState", "RemoteLocation", "SkypeInstant", "SocialProfile", "State", "Street",
	"Suffix", "Title", "UID", "URLs", "X-IDCEE", "YahooInstant", "ZIP",
	"_$!<Anniversary>!$_", "_$!<Assistant>!$_", "_$!<Brother>!$_", "_$!<Child>!$_",
	"_$!<Father>!$_", "_$!<Friend>!$_", "_$!<Home>!$_", "_$!<HomeFAX>!$_",
	"_$!<HomePage>!$_", "_$!<Main>!$_", "_$!<Manager>!$_", "_$!<Mobile>!$_",
	"_$!<Mother>!$_", "_$!<Other>!$_", "_$!<Pager>!$_", "_$!<Parent>!$_",
	"_$!<Partner>!$_", "_$!<Sister>!$_", "_$!<Spouse>!$_", "_$!<Work>!$_",
	"_$!<WorkFAX>!$_", "calendarURIs", "cellPhone", "com.postbox-inc.allowRemoteContent",
	"com.postbox-inc.popularityIndex", "com.yahoo.addressbook.category",
	"com.yahoo.addressbook.custom1", "com.yahoo.addressbook.custom2",
	"com.yahoo.addressbook.custom3", "com.yahoo.addressbook.custom4",
	"com.yahoo.addressbook.customURL", "com.yahoo.addressbook.id", "homeAddress",
	"homeEmail", "homeIM", "homePhone", "kGCKCustomFieldProperty",
	"kGCKIMSkypeProperty", "kGCKLabelProperty", "kGCKRatingProperty", "otherAddress",
	"otherEmail", "otherIM", "url", "work main", "work mobile", "work pager",
	"workAddress", "workEmail", "workIM", "workPhone",
})

func knownAppleAddressBookPropertyType(name string) bool {
	return appleAddressBookPropertyTypes[name]
}

func stringSet(values []string) map[string]bool {
	set := make(map[string]bool, len(values))
	for _, value := range values {
		set[value] = true
	}

	return set
}
