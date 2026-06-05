// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"bytes"
	"encoding/csv"
	"errors"
	"fmt"
	"regexp"
	"strings"

	"blackcat.ca/gmeow/internal/ontology"
)

func csvToRDF(content string) ([]renderedContact, []RecordRejection, error) {
	rows, err := parseCSVRows(content)
	if err != nil {
		return nil, nil, err
	}
	if len(rows) == 0 {
		return nil, nil, errors.New("CSV import has no contacts")
	}

	headerMap, schemaName, err := csvHeaderMap(rows[0].header)
	if err != nil {
		return nil, nil, err
	}

	records := make([]renderedContact, 0, len(rows))
	for index, row := range rows {
		subject := csvContactSubject(row, schemaName, index)
		if subject == "" {
			return nil, nil, fmt.Errorf("CSV row %d has no contact identity", index+2)
		}
		var body strings.Builder
		writeTriple(&body, subject, rdfType, iri(foafPrefix+"Person"))
		for _, header := range row.header {
			value := strings.TrimSpace(row.values[header])
			if value == "" {
				continue
			}
			mapping := headerMap[header]
			if mapping.Predicate == "" {
				return nil, nil, fmt.Errorf("CSV field %q has no mapping", header)
			}
			object := csvMappedObject(mapping, value)
			if object == "" {
				return nil, nil, fmt.Errorf("CSV field %q has invalid value", header)
			}
			writeTriple(&body, subject, mapping.Predicate, object)
			if mapping.ValidFromHeader != "" {
				if validFrom := strings.TrimSpace(
					row.values[mapping.ValidFromHeader],
				); validFrom != "" {
					writeStatementDateAnnotation(
						&body,
						"<< "+iri(subject)+" "+iri(mapping.Predicate)+" "+object+" >> ",
						timePrefix+"hasBeginning",
						validFrom,
					)
				}
			}
			if mapping.ValidUntilHeader != "" {
				if validUntil := strings.TrimSpace(
					row.values[mapping.ValidUntilHeader],
				); validUntil != "" {
					writeTemporalEndAnnotation(&body, subject, mapping.Predicate, object, validUntil)
				}
			}
		}
		records = append(records, renderedContact{identity: subject, body: body.String()})
	}

	return records, nil, nil
}

type csvFieldMapping struct {
	Predicate        string
	ObjectKind       string
	ValidFromHeader  string
	ValidUntilHeader string
}

func parseCSVRows(content string) ([]csvRow, error) {
	content = strings.TrimPrefix(content, "\ufeff")
	delimiter := ','
	firstLine := content
	if index := strings.IndexAny(firstLine, "\r\n"); index >= 0 {
		firstLine = firstLine[:index]
	}
	if strings.Count(firstLine, "\t") > strings.Count(firstLine, ",") {
		delimiter = '\t'
	}
	reader := csv.NewReader(bytes.NewReader([]byte(content)))
	reader.FieldsPerRecord = -1
	reader.LazyQuotes = true
	reader.TrimLeadingSpace = true
	reader.Comma = delimiter
	rows, err := reader.ReadAll()
	if err != nil {
		return nil, fmt.Errorf("parse CSV: %w", err)
	}
	if len(rows) == 0 {
		return nil, errors.New("CSV import is empty")
	}
	header := normalizeCSVHeader(rows[0])
	if len(header) == 0 {
		return nil, errors.New("CSV import has no header")
	}

	result := []csvRow{}
	for index, raw := range rows[1:] {
		if len(raw) == 0 || csvRowIsBlank(raw) {
			continue
		}
		if len(raw) > len(header) && !csvExtraFieldsAreBlank(raw[len(header):]) {
			return nil, fmt.Errorf("CSV row %d has more fields than header", index+2)
		}
		values := map[string]string{}
		for column, name := range header {
			if column < len(raw) {
				values[name] = strings.TrimSpace(raw[column])
			}
		}
		result = append(result, csvRow{header: header, values: values})
	}

	return result, nil
}

func csvExtraFieldsAreBlank(values []string) bool {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}

	return true
}

func normalizeCSVHeader(header []string) []string {
	normalized := []string{}
	seen := map[string]int{}
	for _, value := range header {
		value = strings.TrimSpace(strings.ReplaceAll(value, "\x00", ""))
		if value == "" {
			continue
		}
		seen[value]++
		if seen[value] > 1 {
			value = fmt.Sprintf("%s %d", value, seen[value])
		}
		normalized = append(normalized, value)
	}

	return normalized
}

func csvRowIsBlank(row []string) bool {
	for _, value := range row {
		if strings.TrimSpace(value) != "" {
			return false
		}
	}

	return true
}

func csvHeaderMap(header []string) (map[string]csvFieldMapping, string, error) {
	lower := csvLowerHeaderSet(header)
	var base map[string]csvFieldMapping
	schemaName := ""
	switch {
	case lower["first name"] && lower["last name"] && lower["email address"] &&
		(lower["current company"] || lower["current position"] || lower["connected on"]):
		base = linkedinConnectionCSVFields()
		schemaName = "linkedin-connections"
	case isLinkedInSectionCSV(lower):
		base = linkedInSectionCSVFields(header)
		schemaName = "linkedin-section"
	case lower["name"] && lower["given name"] && lower["family name"]:
		base = googleContactCSVFields()
		schemaName = "google-contacts"
	case lower["e-mail address"] && (lower["business street"] || lower["home street"]):
		base = outlookContactCSVFields()
		schemaName = "outlook-contacts"
	case lower["first"] && lower["last"] && lower["email"]:
		base = yahooContactCSVFields()
		schemaName = "yahoo-contacts"
	case lower["first name"] && lower["last name"] && lower["email"]:
		base = simpleContactCSVFields()
		schemaName = "simple-name-email"
	default:
		return nil, "", errors.New("unsupported CSV contact schema")
	}

	mapping := map[string]csvFieldMapping{}
	for _, field := range header {
		// Canonical grounding wins over the dialect map: a personal-identity column
		// (names/org/title/url/birthday/gender) maps to its standard schema:/foaf:
		// predicate regardless of which CSV dialect emitted it, so the SAME person's
		// name resolves across LinkedIn/Outlook/Yahoo/Google instead of fracturing on
		// source-namespaced predicates (the over-split anti-pattern).
		if canonical, found := canonicalCSVField(field); found {
			mapping[field] = canonical
			continue
		}
		if mapped, found := base[field]; found {
			mapping[field] = mapped
			continue
		}
		if mapped, found := repeatedCSVFieldMapping(field); found {
			mapping[field] = mapped
			continue
		}
		return nil, "", fmt.Errorf("unsupported CSV field %q", field)
	}

	return mapping, schemaName, nil
}

// canonicalCSVField grounds a CSV column whose name denotes a personal-identity
// attribute to its canonical ontology predicate, independent of dialect. It is the
// cross-format convergence point: First Name / First / Given Name all become
// schema:givenName, so records from different CSV exporters describing one person
// share concepts and resolve together. Ambiguous columns (bare "Name" — a person
// in Google but a skill in a LinkedIn section; bare "Title" — honorific vs job) are
// deliberately NOT grounded here; they fall through to the dialect map as
// provenance. Email/phone are already grounded in the dialect maps.
func canonicalCSVField(field string) (csvFieldMapping, bool) {
	switch strings.ToLower(strings.TrimSpace(field)) {
	case "first name", "first", "given name":
		return csvFieldMapping{Predicate: schemaPrefix + "givenName"}, true
	case "last name", "last", "family name", "surname":
		return csvFieldMapping{Predicate: schemaPrefix + "familyName"}, true
	case "middle name", "middle", "additional name":
		return csvFieldMapping{Predicate: schemaPrefix + "additionalName"}, true
	case "nickname", "short name":
		return csvFieldMapping{Predicate: foafPrefix + "nick"}, true
	case "company", "current company", "company name", "organization 1 - name":
		return csvFieldMapping{Predicate: schemaPrefix + "worksFor"}, true
	case "job title", "current position", "position", "headline":
		return csvFieldMapping{Predicate: schemaPrefix + "jobTitle"}, true
	case "birthday":
		return csvFieldMapping{Predicate: schemaPrefix + "birthDate"}, true
	case "gender":
		return csvFieldMapping{Predicate: schemaPrefix + "gender"}, true
	case "url", "web page", "personal web page", "personal website", "business website":
		return csvFieldMapping{Predicate: schemaPrefix + "url", ObjectKind: "iri"}, true
	}

	// Address components (Business/Home/Other and Google "Address N - …" variants)
	// route to the canonical gmeow address predicates so CSV addresses participate
	// in resolution instead of leaking to dropped source-property predicates.
	if pred := csvAddressComponent(strings.ToLower(strings.TrimSpace(field))); pred != "" {
		return csvFieldMapping{Predicate: pred}, true
	}

	return csvFieldMapping{}, false
}

// csvAddressComponent maps an address column name to its canonical gmeow address
// predicate (or "" if not an address field). Ordered so "po box" / "street 2/3"
// win over the broader "street" / "country" matches.
func csvAddressComponent(name string) string {
	switch {
	case strings.Contains(name, "po box"), strings.Contains(name, "post office box"):
		return ontology.PostOfficeBox
	case strings.Contains(name, "street 2"), strings.Contains(name, "street 3"),
		strings.Contains(name, "extended address"):
		return ontology.ExtendedAddress
	case strings.HasSuffix(name, " street"), strings.HasSuffix(name, "- street"),
		strings.HasSuffix(name, " address"): // "business address" / "home address" (whole)
		return ontology.StreetAddress
	case strings.HasSuffix(name, " city"), strings.HasSuffix(name, "- city"):
		return ontology.AddressLocality
	case strings.HasSuffix(name, " state"), strings.HasSuffix(name, "- region"),
		strings.HasSuffix(name, " province"):
		return ontology.AddressRegion
	case strings.Contains(name, "postal code"), strings.HasSuffix(name, " zip"),
		strings.HasSuffix(name, "- postal code"):
		return ontology.PostalCode
	case strings.HasSuffix(name, " country"), strings.HasSuffix(name, "- country"):
		return ontology.CountryCode
	default:
		return ""
	}
}

var repeatedCSVFieldPatterns = []struct {
	pattern   *regexp.Regexp
	predicate string
}{
	{
		regexp.MustCompile(
			`^Address [0-9]+ - (Type|Formatted|Street|City|PO Box|Region|Postal Code|Country|Extended Address)$`,
		),
		gmeowPrefix + "csvAddressField",
	},
	{
		regexp.MustCompile(`^Custom Field [0-9]+ - (Type|Value)$`),
		gmeowPrefix + "csvCustomField",
	},
	{regexp.MustCompile(`^E-mail [0-9]+ - (Type|Value)$`), gmeowPrefix + "csvEmailField"},
	{regexp.MustCompile(`^IM [0-9]+ - (Type|Service|Value)$`), gmeowPrefix + "csvIMField"},
	{regexp.MustCompile(`^Jot [0-9]+ - (Type|Value)$`), gmeowPrefix + "csvJotField"},
	{regexp.MustCompile(`^Messenger ID[0-9]+$`), gmeowPrefix + "csvMessengerID"},
	{
		regexp.MustCompile(
			`^Organization [0-9]+ - (Type|Name|Yomi Name|Title|Department|Symbol|Location|Job Description)$`,
		),
		gmeowPrefix + "csvOrganizationField",
	},
	{regexp.MustCompile(`^Phone [0-9]+ - (Type|Value)$`), gmeowPrefix + "csvPhoneField"},
	{
		regexp.MustCompile(`^Relation [0-9]+ - (Type|Value)$`),
		gmeowPrefix + "csvRelationField",
	},
	{
		regexp.MustCompile(`^Website [0-9]+ - (Type|Value)$`),
		gmeowPrefix + "csvWebsiteField",
	},
}

func repeatedCSVFieldMapping(field string) (csvFieldMapping, bool) {
	for _, entry := range repeatedCSVFieldPatterns {
		if entry.pattern.MatchString(field) {
			return csvFieldMapping{Predicate: entry.predicate}, true
		}
	}

	return csvFieldMapping{}, false
}

func isLinkedInSectionCSV(lower map[string]bool) bool {
	switch {
	case lower["company name"] && lower["start date"] && lower["end date"] && lower["title"]:
		return true
	case lower["school name"] && lower["start date"] && lower["end date"]:
		return true
	case lower["skill name"], lower["course name"], lower["name"] && lower["proficiency"]:
		return true
	case lower["email address"] && lower["status"] && lower["is primary"]:
		return true
	case lower["first name"] && lower["last name"] && lower["headline"] && lower["summary"]:
		return true
	case lower["login date"] && lower["ip address"],
		lower["challenge date"] && lower["challenge type"]:
		return true
	case lower["date"] && lower["share title"] && lower["share link"]:
		return true
	case lower["title"] && lower["patent number"],
		lower["name"] && lower["publisher"] && lower["url"]:
		return true
	case lower["number"] && lower["extension"] && lower["type"]:
		return true
	case lower["type"] && lower["duration"] && lower["device"]:
		return true
	case lower["age group"] && lower["followed companies"]:
		return true
	case lower["registration date"] && lower["subscription type"],
		lower["time registered"] && lower["application id"],
		lower["time"] && lower["event"]:
		return true
	case lower["time"] && lower["search query"]:
		return true
	default:
		return false
	}
}

func csvLowerHeaderSet(header []string) map[string]bool {
	set := map[string]bool{}
	for _, field := range header {
		set[strings.ToLower(field)] = true
	}

	return set
}

func csvContactSubject(row csvRow, schemaName string, index int) string {
	for _, field := range []string{
		"Email Address", "E-mail Address", "Email", "E-mail 2 Address", "E-mail 3 Address",
	} {
		if normalized := normalizeContactEmail(row.values[field]); normalized != "" {
			return "mailto:" + normalized
		}
	}
	for _, field := range []string{"URL", "Web Page", "Personal Website", "Business Website"} {
		if value := strings.TrimSpace(row.values[field]); strings.Contains(value, ":") {
			return value
		}
	}

	key := schemaName + ":" + fmt.Sprintf("%d", index) + ":" + strings.Join([]string{
		row.values["Name"], row.values["First Name"], row.values["First"],
		row.values["Last Name"], row.values["Last"], row.values["Company"],
		row.values["Current Company"],
	}, "|")
	return "urn:gmeow:contact:csv:" + shortHash([]byte(key))
}

func csvMappedObject(mapping csvFieldMapping, value string) string {
	switch mapping.ObjectKind {
	case "iri":
		if strings.Contains(value, ":") {
			return iri(value)
		}
		return literal(value)
	case "email":
		if object := normalizedEmailIRI(value); object != "" {
			return object
		}
		return literal(value)
	default:
		return literal(value)
	}
}

func simpleContactCSVFields() map[string]csvFieldMapping {
	return map[string]csvFieldMapping{
		"First Name": {Predicate: gmeowPrefix + "csvFirstName"},
		"Last Name":  {Predicate: gmeowPrefix + "csvLastName"},
		"Email":      {Predicate: schemaPrefix + "email", ObjectKind: "email"},
	}
}

func linkedinConnectionCSVFields() map[string]csvFieldMapping {
	return map[string]csvFieldMapping{
		"First Name":       {Predicate: gmeowPrefix + "linkedInFirstName"},
		"Last Name":        {Predicate: gmeowPrefix + "linkedInLastName"},
		"Email Address":    {Predicate: schemaPrefix + "email", ObjectKind: "email"},
		"Current Company":  {Predicate: gmeowPrefix + "linkedInCurrentCompany"},
		"Current Position": {Predicate: gmeowPrefix + "linkedInCurrentPosition"},
		"Company":          {Predicate: gmeowPrefix + "linkedInCurrentCompany"},
		"Position":         {Predicate: gmeowPrefix + "linkedInCurrentPosition"},
		"URL": {
			Predicate:  gmeowPrefix + "linkedInProfileURL",
			ObjectKind: "iri",
		},
		"Connected On": {Predicate: gmeowPrefix + "linkedInConnectedOn"},
	}
}

func googleContactCSVFields() map[string]csvFieldMapping {
	fields := map[string]csvFieldMapping{}
	for _, field := range []string{
		"Name", "Given Name", "Family Name", "Additional Name", "Yomi Name",
		"Given Name Yomi", "Additional Name Yomi", "Family Name Yomi", "Name Prefix",
		"Name Suffix", "Initials", "Nickname", "Short Name", "Maiden Name", "Birthday",
		"Gender", "Location", "Billing Information", "Directory Server", "Mileage",
		"Occupation", "Hobby", "Sensitivity", "Priority", "Subject", "Notes", "Language",
		"Group Membership", "Organization 1 - Name", "Organization 1 - Title",
		"Organization 1 - Department", "Organization 1 - Symbol", "Organization 1 - Location",
		"Organization 1 - Job Description", "Relation 1 - Type", "Relation 1 - Value",
		"Website 1 - Type", "Website 1 - Value", "Event 1 - Type", "Event 1 - Value",
	} {
		fields[field] = csvFieldMapping{
			Predicate: gmeowPrefix + "googleContacts" + compactPredicateName(field),
		}
	}
	for _, field := range []string{
		"E-mail 1 - Value", "E-mail 2 - Value", "E-mail 3 - Value", "E-mail 4 - Value",
	} {
		fields[field] = csvFieldMapping{
			Predicate:  schemaPrefix + "email",
			ObjectKind: "email",
		}
	}
	for _, field := range []string{
		"Phone 1 - Value", "Phone 2 - Value", "Phone 3 - Value", "Phone 4 - Value",
		"Phone 5 - Value", "Phone 6 - Value",
	} {
		fields[field] = csvFieldMapping{Predicate: schemaPrefix + "telephone"}
	}
	for _, field := range []string{
		"Address 1 - Formatted", "Address 1 - Street", "Address 1 - City",
		"Address 1 - PO Box", "Address 1 - Region", "Address 1 - Postal Code",
		"Address 1 - Country", "Address 1 - Extended Address",
	} {
		fields[field] = csvFieldMapping{
			Predicate: gmeowPrefix + "googleContacts" + compactPredicateName(field),
		}
	}
	addRepeatedCSVFields(fields, "E-mail", 12, []string{"Type", "Value"})
	addRepeatedCSVFields(fields, "Phone", 16, []string{"Type", "Value"})
	addRepeatedCSVFields(fields, "IM", 12, []string{"Type", "Service", "Value"})
	addRepeatedCSVFields(fields, "Jot", 12, []string{"Type", "Value"})
	addRepeatedCSVFields(
		fields,
		"Address",
		12,
		[]string{
			"Type",
			"Formatted",
			"Street",
			"City",
			"PO Box",
			"Region",
			"Postal Code",
			"Country",
			"Extended Address",
		},
	)
	addRepeatedCSVFields(
		fields,
		"Organization",
		6,
		[]string{
			"Type",
			"Name",
			"Yomi Name",
			"Title",
			"Department",
			"Symbol",
			"Location",
			"Job Description",
		},
	)
	addRepeatedCSVFields(fields, "Website", 32, []string{"Type", "Value"})
	addRepeatedCSVFields(fields, "Relation", 16, []string{"Type", "Value"})
	addRepeatedCSVFields(fields, "Event", 6, []string{"Type", "Value"})
	addRepeatedCSVFields(fields, "Custom Field", 12, []string{"Type", "Value"})

	return fields
}

func outlookContactCSVFields() map[string]csvFieldMapping {
	fields := map[string]csvFieldMapping{}
	for _, field := range []string{
		"Title", "First Name", "Middle Name", "Last Name", "Suffix", "Company",
		"Department", "Job Title", "Categories", "Children", "Company Yomi", "Directory Server", "E-mail Type", "Organizational ID Number", "Profession", "Business Address PO Box", "Business Street", "Business Street 2",
		"Business Street 3", "Business City", "Business State", "Business Postal Code",
		"Business Country", "Home Address", "Home Address PO Box", "Home Street", "Home Street 2", "Home Street 3", "Home City",
		"Home State", "Home Postal Code", "Home Country", "Other Street", "Other Street 2",
		"Other Street 3", "Other City", "Other State", "Other Postal Code", "Other Country",
		"Other Address PO Box",
		"Notes", "Birthday", "Anniversary", "Web Page", "Gender", "Initials",
		"Office Location", "Language", "Internet Free Busy", "Location", "Personal Web Page", "Referred By", "Spouse",
		"ISDN", "Account", "Assistant's Name", "Manager's Name", "Billing Information",
		"Business Address", "E-mail Display Name", "Skype ID",
		"E-mail 2 Type", "E-mail 2 Display Name", "E-mail 3 Type", "E-mail 3 Display Name",
		"Given Yomi", "Government ID Number", "Hobby", "Keywords", "Mileage", "Other Address",
		"Priority", "Private", "Sensitivity", "Surname Yomi", "User 1", "User 2", "User 3", "User 4",
	} {
		fields[field] = csvFieldMapping{
			Predicate: gmeowPrefix + "outlookContacts" + compactPredicateName(field),
		}
	}
	for _, field := range []string{"E-mail Address", "E-mail 2 Address", "E-mail 3 Address"} {
		fields[field] = csvFieldMapping{
			Predicate:  schemaPrefix + "email",
			ObjectKind: "email",
		}
	}
	for _, field := range []string{
		"Primary Phone", "Home Phone", "Home Phone 2", "Mobile Phone", "Pager",
		"Home Fax", "Business Phone", "Business Phone 2", "Business Fax",
		"Company Main Phone", "Other Phone", "Other Fax", "Car Phone", "Radio Phone",
		"TTY/TDD Phone", "Telex", "Callback", "Assistant's Phone",
	} {
		fields[field] = csvFieldMapping{Predicate: schemaPrefix + "telephone"}
	}

	return fields
}

func yahooContactCSVFields() map[string]csvFieldMapping {
	fields := map[string]csvFieldMapping{}
	for _, field := range []string{
		"First", "Middle", "Last", "Nickname", "Category", "Distribution Lists",
		"Messenger ID", "Messenger ID1", "Messenger ID2", "Messenger ID3", "Messenger ID4", "Messenger ID5", "Messenger ID6", "Messenger ID7", "Messenger ID8", "Messenger ID9", "Home", "Work", "Pager", "Fax", "Mobile", "Other", "Yahoo Phone",
		"Yahoo! Phone", "Primary", "Personal Website", "Business Website", "Title",
		"Company", "Work Address", "Work City", "Work State", "Work ZIP", "Work Country",
		"Home Address", "Home City", "Home State", "Home ZIP", "Home Country", "Birthday",
		"Anniversary", "Custom 1", "Custom 2", "Custom 3", "Custom 4", "Comments",
		"Skype ID", "IRC ID", "ICQ ID", "Google ID", "MSN ID", "AIM ID", "QQ ID",
	} {
		fields[field] = csvFieldMapping{
			Predicate: gmeowPrefix + "yahooContacts" + compactPredicateName(field),
		}
	}
	for _, field := range []string{"Email", "Alternate Email 1", "Alternate Email 2"} {
		fields[field] = csvFieldMapping{
			Predicate:  schemaPrefix + "email",
			ObjectKind: "email",
		}
	}

	return fields
}

func linkedInSectionCSVFields(header []string) map[string]csvFieldMapping {
	fields := map[string]csvFieldMapping{}
	for _, field := range knownLinkedInSectionCSVFields() {
		mapping := csvFieldMapping{
			Predicate: gmeowPrefix + "linkedIn" + compactPredicateName(field),
		}
		switch strings.ToLower(field) {
		case "email address":
			mapping = csvFieldMapping{Predicate: schemaPrefix + "email", ObjectKind: "email"}
		case "url", "share link", "shared url", "media url":
			mapping.ObjectKind = "iri"
		case "number":
			mapping.Predicate = schemaPrefix + "telephone"
		}
		fields[field] = mapping
	}

	return fields
}

func knownLinkedInSectionCSVFields() []string {
	return []string{
		"Account Status",
		"Action",
		"Activities",
		"Address",
		"Age Group",
		"Application Id",
		"Application Number",
		"Association",
		"Birth Date",
		"Browser",
		"Challenge Date",
		"Challenge Type",
		"Companies",
		"Company",
		"Company Name",
		"Company Sizes",
		"Connected On",
		"Connection Degree",
		"Contact Instructions",
		"Content",
		"Country",
		"Course Name",
		"Course Number",
		"Created Date",
		"Date",
		"Date Added On",
		"Degree Classes",
		"Degree Name",
		"Description",
		"Device",
		"Duration",
		"Email Address",
		"End Date",
		"Event",
		"Extension",
		"Field Of Study",
		"Filing Date",
		"First Name",
		"Followed Companies",
		"Followed Industries",
		"Functions",
		"Gender",
		"Geo Location",
		"Graduation Year",
		"Groups",
		"Headline",
		"Industries",
		"Industry",
		"Interface Language",
		"Inviter First Name",
		"Inviter Last Name",
		"IP Address",
		"Is Primary",
		"Issue Date",
		"Issuer",
		"Last Name",
		"Location",
		"Login Date",
		"Login Type",
		"Maiden Name",
		"Marital Status",
		"Media URL",
		"Mobile Device Type",
		"Mobile Model",
		"Name",
		"Notes",
		"Number",
		"Partner Opt Out Advertising",
		"Patent Number",
		"Phone Type",
		"Proficiency",
		"Publisher",
		"Registration Date",
		"Registration IP",
		"School Name",
		"Schools",
		"Search Query",
		"Seniorities",
		"Share Commentary",
		"Share Description",
		"Share Link",
		"Share Title",
		"Shared URL",
		"Shared Url",
		"Skill Name",
		"Skills",
		"Start Date",
		"State",
		"Status",
		"Subscription Type",
		"Summary",
		"Time",
		"Time Registered",
		"Title",
		"Type",
		"URL",
		"Url",
		"User Agent",
		"Visibility",
		"Zip Code",
	}
}

func addRepeatedCSVFields(
	fields map[string]csvFieldMapping,
	prefix string,
	limit int,
	suffixes []string,
) {
	for index := 1; index <= limit; index++ {
		for _, suffix := range suffixes {
			field := fmt.Sprintf("%s %d - %s", prefix, index, suffix)
			fields[field] = csvFieldMapping{
				Predicate: gmeowPrefix + "csv" + compactPredicateName(field),
			}
		}
	}
}

func compactPredicateName(value string) string {
	var builder strings.Builder
	upperNext := true
	for _, char := range value {
		switch {
		case char >= 'a' && char <= 'z':
			if upperNext {
				char -= 'a' - 'A'
			}
			builder.WriteRune(char)
			upperNext = false
		case char >= 'A' && char <= 'Z', char >= '0' && char <= '9':
			builder.WriteRune(char)
			upperNext = false
		default:
			upperNext = true
		}
	}

	return builder.String()
}
