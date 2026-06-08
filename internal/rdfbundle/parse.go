// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

// Package rdfbundle is a dependency-free Turtle/RDF-star bundle parser. It turns
// a contact bundle's Turtle text — either a raw rooted graph (e.g. index.ttl,
// with @prefix headers, predicate/object lists and `;`/`,` shorthand) or the
// line-oriented bodies the importer generates — into flat statements and
// RDF-star annotations.
//
// It is shared by the QUERY projection (which inserts the statements into the
// RDF tables) and by IMPORT (which derives claim statements for ingest-time
// entity resolution). Keeping it free of QUERY/FILESTORE dependencies lets the
// importer parse bundles without violating the FILESTORE-only-import invariant.
package rdfbundle

import (
	"bufio"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"strings"
	"unicode"
)

const (
	prefixFieldCount      = 3
	rdfScannerBufferSize  = 4096
	rdfScannerMaxCapacity = 1024 * 1024
	splitQuoteOffset      = 2

	// TypePredicate is the rdf:type IRI, exported because callers (the projection)
	// special-case it.
	TypePredicate = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
)

// Term is an RDF term (IRI or literal) with optional literal language/datatype.
type Term struct {
	Kind     string
	Value    string
	Language string
	Datatype string
}

// Statement is a flat subject-predicate-object triple plus a stable content
// Hash (subject+predicate+object, independent of source).
type Statement struct {
	Subject   Term
	Predicate Term
	Object    Term
	Hash      string
}

// bundleParser accumulates statements and annotations across lines.
type bundleParser struct {
	currentPredicate  Term
	currentSubject    Term
	prefixes          map[string]string
	currentAnnotation string
	annotations       []AnnotationRecord
	statements        []Statement
	continuingObjects bool
}

// AnnotationRecord is an RDF-star annotation hung off a statement (by Hash).
type AnnotationRecord struct {
	Statement Statement
	Predicate Term
	Object    Term
	Hash      string
}

// Parse scans a Turtle/RDF-star bundle into statements and annotations.
func Parse(content string) ([]Statement, []AnnotationRecord, error) {
	parser := bundleParser{
		prefixes: map[string]string{
			"rdf": "http://www.w3.org/1999/02/22-rdf-syntax-ns#",
		},
	}
	scanner := bufio.NewScanner(strings.NewReader(content))
	scanner.Buffer(make([]byte, rdfScannerBufferSize), rdfScannerMaxCapacity)

	for scanner.Scan() {
		parser.consume(scanner.Text())
	}

	err := scanner.Err()
	if err != nil {
		return nil, nil, fmt.Errorf("scan RDF bundle: %w", err)
	}

	return parser.statements, parser.annotations, nil
}

func (parser *bundleParser) consume(raw string) {
	indented := raw != "" && unicode.IsSpace(rune(raw[0]))

	line := strings.TrimSpace(raw)
	if line == "" || strings.HasPrefix(line, "#") {
		return
	}

	if parsePrefix(line, parser.prefixes) {
		return
	}

	if parser.consumeAnnotationContinuation(line, indented) ||
		parser.consumeRDFStar(line) ||
		parser.consumeObjectContinuation(line) ||
		parser.consumeIndentedPredicate(line, indented) ||
		parser.consumeSubject(line) {
		return
	}

	parser.consumePredicate(line)
}

func (parser *bundleParser) consumeAnnotationContinuation(
	line string,
	indented bool,
) bool {
	if parser.currentAnnotation == "" || !indented {
		return false
	}

	next := parseAnnotationLine(parser.currentAnnotation, line, parser.prefixes)
	parser.annotations = append(parser.annotations, next...)

	if strings.HasSuffix(line, ".") {
		parser.currentAnnotation = ""
	}

	return true
}

func (parser *bundleParser) consumeRDFStar(line string) bool {
	if !strings.HasPrefix(line, "<<") {
		return false
	}

	annotation, found := parseStarAnnotation(line, parser.prefixes)
	if found {
		parser.statements = append(parser.statements, annotation.Statement)
		parser.annotations = append(parser.annotations, annotation)

		return true
	}

	statement, found := parseStarSubject(line, parser.prefixes)
	if found {
		parser.statements = append(parser.statements, statement)
		parser.currentAnnotation = statement.Hash
	}

	return true
}

func (parser *bundleParser) consumeObjectContinuation(line string) bool {
	if !parser.continuingObjects {
		return false
	}

	next, found := parseObjectList(
		parser.currentSubject,
		parser.currentPredicate,
		line,
		parser.prefixes,
	)
	if !found {
		return false
	}

	parser.statements = append(parser.statements, next...)
	parser.continuingObjects = strings.HasSuffix(line, ",")

	if strings.HasSuffix(line, ".") || strings.HasSuffix(line, ";") {
		parser.continuingObjects = false
	}

	return true
}

func (parser *bundleParser) consumeIndentedPredicate(line string, indented bool) bool {
	if parser.currentSubject.Value == "" || !indented {
		return false
	}

	return parser.consumePredicate(line)
}

func (parser *bundleParser) consumeSubject(line string) bool {
	subject, rest, found := parseSubjectLine(line, parser.prefixes)
	if !found {
		return false
	}

	parser.currentSubject = subject
	parser.currentPredicate = Term{}
	parser.continuingObjects = false

	if strings.TrimSpace(rest) == "" {
		return true
	}

	parser.consumePredicate(rest)

	return true
}

func (parser *bundleParser) consumePredicate(line string) bool {
	if parser.currentSubject.Value == "" {
		return false
	}

	predicate, next, found := parsePredicateObjectLine(
		parser.currentSubject,
		line,
		parser.prefixes,
	)
	if !found {
		return false
	}

	parser.currentPredicate = predicate
	parser.statements = append(parser.statements, next...)
	parser.continuingObjects = strings.HasSuffix(line, ",")

	return true
}

func parsePrefix(line string, prefixes map[string]string) bool {
	if !strings.HasPrefix(line, "@prefix ") {
		return false
	}

	fields := strings.Fields(line)
	if len(fields) < prefixFieldCount {
		return true
	}

	name := strings.TrimSuffix(fields[1], ":")

	value := strings.Trim(fields[2], "<>")
	if name != "" && value != "" {
		prefixes[name] = value
	}

	return true
}

func parseSubjectLine(line string, prefixes map[string]string) (Term, string, bool) {
	first, rest := splitFirstToken(line)
	if first == "" {
		return Term{}, "", false
	}

	if !looksLikeTerm(first) {
		return Term{}, "", false
	}

	subject, ok := parseTerm(first, prefixes)
	if !ok || subject.Kind == "literal" {
		return Term{}, "", false
	}

	return subject, rest, true
}

func parsePredicateObjectLine(
	subject Term,
	line string,
	prefixes map[string]string,
) (Term, []Statement, bool) {
	predicateToken, rest := splitFirstToken(line)

	predicate, parsed := parsePredicateTerm(predicateToken, prefixes)
	if !parsed {
		return Term{}, nil, false
	}

	statements, parsed := parseObjectList(subject, predicate, rest, prefixes)
	if !parsed {
		return Term{}, nil, false
	}

	return predicate, statements, true
}

func parseObjectList(
	subject, predicate Term,
	line string,
	prefixes map[string]string,
) ([]Statement, bool) {
	line = strings.TrimSpace(line)
	line = strings.TrimSuffix(line, ";")
	line = strings.TrimSuffix(line, ".")
	items := splitObjects(line)
	statements := []Statement{}

	for _, item := range items {
		object, ok := parseTerm(item, prefixes)
		if !ok {
			continue
		}

		statement := Statement{Subject: subject, Predicate: predicate, Object: object}
		statement.Hash = StatementHash(statement)
		statements = append(statements, statement)
	}

	return statements, len(statements) > 0
}

func parseStarAnnotation(
	line string,
	prefixes map[string]string,
) (AnnotationRecord, bool) {
	statement, rest, cutFound := parseStarStatement(line, prefixes)
	if !cutFound {
		return AnnotationRecord{}, false
	}

	annotationPredicateToken, annotationObjectText := splitFirstToken(rest)

	annotationPredicate, parsed := parsePredicateTerm(annotationPredicateToken, prefixes)
	if !parsed {
		return AnnotationRecord{}, false
	}

	annotationObject, parsed := parseTerm(annotationObjectText, prefixes)
	if !parsed {
		return AnnotationRecord{}, false
	}

	return AnnotationRecord{
		Statement: statement,
		Hash:      StatementHash(statement),
		Predicate: annotationPredicate,
		Object:    annotationObject,
	}, true
}

func parseStarSubject(line string, prefixes map[string]string) (Statement, bool) {
	statement, rest, cutFound := parseStarStatement(line, prefixes)
	if !cutFound || strings.TrimSpace(strings.TrimSuffix(rest, ";")) != "" {
		return Statement{}, false
	}

	return statement, true
}

func parseStarStatement(
	line string,
	prefixes map[string]string,
) (Statement, string, bool) {
	inner, rest, cutFound := strings.Cut(strings.TrimPrefix(line, "<<"), ">>")
	if !cutFound {
		return Statement{}, "", false
	}

	subjectToken, restInner := splitFirstToken(inner)
	predicateToken, objectText := splitFirstToken(restInner)

	subject, parsed := parseTerm(subjectToken, prefixes)
	if !parsed {
		return Statement{}, "", false
	}

	predicate, parsed := parsePredicateTerm(predicateToken, prefixes)
	if !parsed {
		return Statement{}, "", false
	}

	object, parsed := parseTerm(objectText, prefixes)
	if !parsed {
		return Statement{}, "", false
	}

	statement := Statement{Subject: subject, Predicate: predicate, Object: object}
	statement.Hash = StatementHash(statement)

	return statement, rest, true
}

func parseAnnotationLine(
	statementHash, line string,
	prefixes map[string]string,
) []AnnotationRecord {
	predicateToken, rest := splitFirstToken(line)

	predicate, ok := parsePredicateTerm(predicateToken, prefixes)
	if !ok {
		return nil
	}

	rest = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(rest), ";"), ".")
	annotations := []AnnotationRecord{}

	for _, item := range splitObjects(rest) {
		object, ok := parseTerm(item, prefixes)
		if !ok {
			continue
		}

		annotations = append(annotations, AnnotationRecord{
			Hash:      statementHash,
			Predicate: predicate,
			Object:    object,
		})
	}

	return annotations
}

func parsePredicateTerm(token string, prefixes map[string]string) (Term, bool) {
	if token == "a" {
		return Term{Kind: "iri", Value: TypePredicate}, true
	}

	return parseTerm(token, prefixes)
}

func parseTerm(token string, prefixes map[string]string) (Term, bool) {
	token = cleanToken(token)
	if token == "" || strings.HasPrefix(token, "[") {
		return Term{}, false
	}

	if strings.HasPrefix(token, "<") && strings.Contains(token, ">") {
		value, _, _ := strings.Cut(strings.TrimPrefix(token, "<"), ">")

		return Term{Kind: "iri", Value: value}, true
	}

	if strings.HasPrefix(token, "\"") {
		return parseLiteralTerm(token, prefixes), true
	}

	if prefix, suffix, ok := strings.Cut(token, ":"); ok {
		if base := prefixes[prefix]; base != "" {
			return Term{Kind: "iri", Value: base + suffix}, true
		}
	}

	return Term{}, false
}

func parseLiteralTerm(token string, prefixes map[string]string) Term {
	value, suffix := splitLiteralValue(token)

	term := Term{Kind: "literal", Value: value}
	if after, ok := strings.CutPrefix(suffix, "@"); ok {
		term.Language = after
	}

	if after, ok := strings.CutPrefix(suffix, "^^"); ok {
		term.Datatype = parseDatatypeIRI(after, prefixes)
	}

	return term
}

func parseDatatypeIRI(token string, prefixes map[string]string) string {
	term, ok := parseTerm(token, prefixes)
	if ok && term.Kind == "iri" {
		return term.Value
	}

	return strings.Trim(token, "<>")
}

func splitLiteralValue(token string) (string, string) {
	value := strings.TrimPrefix(token, "\"")
	escaped := false

	for index, char := range value {
		if escaped {
			escaped = false

			continue
		}

		if char == '\\' {
			escaped = true

			continue
		}

		if char == '"' {
			return value[:index], value[index+1:]
		}
	}

	return value, ""
}

func cleanToken(token string) string {
	token = strings.TrimSpace(token)
	token = strings.TrimSuffix(token, ",")
	token = strings.TrimSuffix(token, ";")
	token = strings.TrimSuffix(token, ".")

	return strings.TrimSpace(token)
}

func splitFirstToken(line string) (string, string) {
	line = strings.TrimSpace(line)
	if line == "" {
		return "", ""
	}

	if strings.HasPrefix(line, "\"") {
		return splitQuotedToken(line)
	}

	for index, char := range line {
		if unicode.IsSpace(char) {
			return line[:index], strings.TrimSpace(line[index:])
		}
	}

	return line, ""
}

func splitQuotedToken(line string) (string, string) {
	escaped := false

	for index, char := range line[1:] {
		if escaped {
			escaped = false

			continue
		}

		if char == '\\' {
			escaped = true

			continue
		}

		if char != '"' {
			continue
		}

		end := index + splitQuoteOffset
		for end < len(line) && !isASCIIWhitespace(line[end]) {
			end++
		}

		return line[:end], strings.TrimSpace(line[end:])
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

func splitObjects(line string) []string {
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

func looksLikeTerm(token string) bool {
	return strings.HasPrefix(token, "<") ||
		strings.HasPrefix(token, "_:") ||
		strings.Contains(token, ":")
}

// StatementHash is the stable content hash of a statement (subject kind+value,
// predicate value, object kind+value+language+datatype).
func StatementHash(statement Statement) string {
	parts := []string{
		statement.Subject.Kind,
		statement.Subject.Value,
		statement.Predicate.Value,
		statement.Object.Kind,
		statement.Object.Value,
		statement.Object.Language,
		statement.Object.Datatype,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))

	return hex.EncodeToString(sum[:])
}

// TermKey is a stable identity key for a term (all four fields).
func TermKey(term Term) string {
	parts := []string{term.Kind, term.Value, term.Language, term.Datatype}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))

	return hex.EncodeToString(sum[:])
}
