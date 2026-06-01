// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package filestore

import (
	"mime"
	"path/filepath"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	DictionaryFamilyOpaqueBinary        = "opaque-binary"
	DictionaryFamilyMailHeaders         = "mail-rfc822-headers"
	DictionaryFamilyMailBodyPlain       = "mail-body-plain"
	DictionaryFamilyMailBodyHTML        = "mail-body-html"
	DictionaryFamilyGmeowMailJSON       = "gmeow-mail-json"
	DictionaryFamilyPatchText           = "patch-text"
	DictionaryFamilyStructuredJSON      = "structured-json"
	DictionaryFamilyStructuredXML       = "structured-xml"
	DictionaryFamilyYAMLToml            = "yaml-toml"
	DictionaryFamilyHTMLCSSJS           = "html-css-js"
	DictionaryFamilySourceCode          = "source-code"
	DictionaryFamilyLogsLineOriented    = "logs-line-oriented"
	DictionaryFamilyCSVTSV              = "csv-tsv"
	DictionaryFamilyRDFTurtle           = "rdf-turtle"
	DictionaryFamilyBinarySerialization = "protobuf-msgpack-cbor-avro"
)

var defaultEnabledDictionaryFamilies = map[string]bool{
	DictionaryFamilyMailHeaders:   true,
	DictionaryFamilyMailBodyPlain: true,
	DictionaryFamilyMailBodyHTML:  true,
	DictionaryFamilyGmeowMailJSON: true,
	DictionaryFamilyPatchText:     true,
}

var trainableDictionaryFamilies = []string{
	DictionaryFamilyMailHeaders,
	DictionaryFamilyMailBodyPlain,
	DictionaryFamilyMailBodyHTML,
	DictionaryFamilyGmeowMailJSON,
	DictionaryFamilyPatchText,
	DictionaryFamilyStructuredJSON,
	DictionaryFamilyStructuredXML,
	DictionaryFamilyYAMLToml,
	DictionaryFamilyHTMLCSSJS,
	DictionaryFamilySourceCode,
	DictionaryFamilyLogsLineOriented,
	DictionaryFamilyCSVTSV,
	DictionaryFamilyRDFTurtle,
	DictionaryFamilyBinarySerialization,
}

func dictionaryFamilyForObject(mediaType string, contentRoles []string) string {
	normalized := normalizedMediaType(mediaType)
	if normalized == "" {
		return DictionaryFamilyOpaqueBinary
	}

	if hasRole(contentRoles, contracts.MailHeadersRole) {
		return DictionaryFamilyMailHeaders
	}
	if hasRole(contentRoles, contracts.MailBodyRole) {
		if normalized == "text/html" {
			return DictionaryFamilyMailBodyHTML
		}

		return DictionaryFamilyMailBodyPlain
	}
	if hasRole(contentRoles, contracts.MailPatchDiffRole) {
		return DictionaryFamilyPatchText
	}
	if hasRole(contentRoles, contracts.MailMIMEStructureRole) ||
		hasRole(contentRoles, contracts.MailMessageVariantRole) ||
		hasRole(contentRoles, contracts.MailArchiveMembershipRole) ||
		hasRole(contentRoles, contracts.MailArchiveMetadataRole) ||
		hasRole(contentRoles, contracts.VersionRecordRole) {
		return DictionaryFamilyGmeowMailJSON
	}

	switch normalized {
	case "message/rfc822", "text/rfc822-headers":
		return DictionaryFamilyMailHeaders
	case "text/x-gmeow-patch":
		return DictionaryFamilyPatchText
	case "application/vnd.gmeow.mail-message+json",
		"application/vnd.gmeow.gmail-message+json",
		"application/vnd.gmeow.mail-message-variant+json",
		"application/vnd.gmeow.mime-structure+json",
		"application/vnd.gmeow.version-record+json",
		"application/vnd.gmeow.mail-archive-membership+json":
		return DictionaryFamilyGmeowMailJSON
	case "text/turtle", "application/n-triples", "application/ld+json":
		return DictionaryFamilyRDFTurtle
	case "application/json":
		return DictionaryFamilyStructuredJSON
	case "application/xml", "text/xml", "application/xhtml+xml", "image/svg+xml":
		return DictionaryFamilyStructuredXML
	case "text/html", "text/css", "text/javascript", "application/javascript":
		return DictionaryFamilyHTMLCSSJS
	case "text/csv":
		return DictionaryFamilyCSVTSV
	case "application/x-protobuf", "application/protobuf", "application/msgpack",
		"application/cbor", "application/avro":
		return DictionaryFamilyBinarySerialization
	}

	if strings.HasSuffix(normalized, "+json") {
		return DictionaryFamilyStructuredJSON
	}
	if strings.HasSuffix(normalized, "+xml") {
		return DictionaryFamilyStructuredXML
	}
	if strings.HasSuffix(normalized, "+cbor") ||
		strings.HasSuffix(normalized, "+msgpack") ||
		strings.HasSuffix(normalized, "+avro") {
		return DictionaryFamilyBinarySerialization
	}
	if isDictionaryIneligibleMediaType(normalized) {
		return DictionaryFamilyOpaqueBinary
	}
	if isSourceCodeMediaType(normalized) {
		return DictionaryFamilySourceCode
	}
	if strings.HasPrefix(normalized, "text/") {
		return genericTextFamily(normalized)
	}

	return DictionaryFamilyOpaqueBinary
}

func normalizedMediaType(mediaType string) string {
	parsed, _, err := mime.ParseMediaType(strings.TrimSpace(mediaType))
	if err != nil {
		parsed = strings.TrimSpace(mediaType)
	}

	return strings.ToLower(parsed)
}

func hasRole(roles []string, target string) bool {
	for _, role := range roles {
		if strings.EqualFold(strings.TrimSpace(role), target) {
			return true
		}
	}

	return false
}

func isDictionaryIneligibleMediaType(mediaType string) bool {
	if mediaType == "application/octet-stream" ||
		strings.HasPrefix(mediaType, "image/") ||
		strings.HasPrefix(mediaType, "audio/") ||
		strings.HasPrefix(mediaType, "video/") ||
		strings.HasPrefix(mediaType, "font/") {
		return true
	}

	switch mediaType {
	case "application/pdf",
		"application/zip",
		"application/gzip",
		"application/x-gzip",
		"application/x-7z-compressed",
		"application/x-xz",
		"application/x-bzip2",
		"application/zstd",
		"application/vnd.rar",
		"application/vnd.ms-cab-compressed",
		"application/vnd.openxmlformats-officedocument.wordprocessingml.document",
		"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
		"application/vnd.openxmlformats-officedocument.presentationml.presentation",
		"application/epub+zip",
		"application/jar":
		return true
	}

	return strings.HasSuffix(mediaType, "+zip")
}

func isSourceCodeMediaType(mediaType string) bool {
	switch mediaType {
	case "text/x-go", "text/x-python", "text/x-rust", "text/x-java-source",
		"text/x-c", "text/x-c++", "text/x-csharp", "text/x-shellscript",
		"text/x-sql", "application/sql", "application/graphql":
		return true
	}

	return false
}

func genericTextFamily(mediaType string) string {
	switch mediaType {
	case "text/tab-separated-values":
		return DictionaryFamilyCSVTSV
	case "text/yaml", "text/x-yaml", "text/toml":
		return DictionaryFamilyYAMLToml
	case "text/markdown", "text/x-rst", "text/x-log":
		return DictionaryFamilyLogsLineOriented
	}

	return DictionaryFamilyLogsLineOriented
}

func dictionaryFamilyEligibleForTraining(family string) bool {
	if family == "" || family == DictionaryFamilyOpaqueBinary {
		return false
	}
	for _, candidate := range trainableDictionaryFamilies {
		if family == candidate {
			return true
		}
	}

	return false
}

func sanitizeDictionaryFamily(family string) string {
	family = strings.TrimSpace(strings.ToLower(family))
	if family == "" {
		return ""
	}

	return filepath.Base(family)
}
