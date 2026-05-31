// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"bufio"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/mail"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/query"
)

const (
	contactFactAffiliation  = "affiliation"
	contactFactAlias        = "alias"
	contactFactAddress      = "address"
	contactFactAccount      = "account"
	contactFactEmail        = "email"
	contactFactIdentifier   = "identifier"
	contactFactName         = "name"
	contactFactPhone        = "phone"
	contactFactRelationship = "relationship"
	contactFactTitle        = "title"
	contactFactURL          = "url"
	prefixFieldCount        = 3
	rdfScannerBufferSize    = 4096
	rdfScannerMaxCapacity   = 1024 * 1024
	rdfTypePredicate        = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	splitQuoteOffset        = 2
)

type rdfTerm struct {
	kind     string
	value    string
	language string
	datatype string
}

type rdfStatement struct {
	subject   rdfTerm
	predicate rdfTerm
	object    rdfTerm
	hash      string
}

type rdfAnnotation struct {
	statement rdfStatement
	predicate rdfTerm
	object    rdfTerm
	hash      string
}

type sourceStatementKey struct {
	sourceDigest  contracts.ObjectDigest
	statementHash string
}

type transactionBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type contactSearchCounter interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

type contactProjectionStatement struct {
	sourceDigest  contracts.ObjectDigest
	statementHash string
	subject       string
	predicate     string
	object        string
	objectKind    string
}

type contactProjectionFact struct {
	sourceDigest  contracts.ObjectDigest
	statementHash string
	contactID     string
	factKind      string
	value         string
	predicate     string
	validFrom     string
	validUntil    string
	historical    bool
}

type rdfBundleParser struct {
	currentPredicate  rdfTerm
	currentSubject    rdfTerm
	prefixes          map[string]string
	currentAnnotation string
	annotations       []rdfAnnotation
	statements        []rdfStatement
	continuingObjects bool
}

func insertRDFRows(
	ctx context.Context,
	transaction pgx.Tx,
	manifest contracts.Manifest,
	source query.ProjectionSource,
) (bool, error) {
	if !manifestHasFacetKind(manifest, contracts.RDFSourceBundleFacetKind) &&
		!manifestHasFacetKind(manifest, contracts.RDFClaimBundleFacetKind) {
		return false, nil
	}

	reader, ok := source.(embeddingObjectReader)
	if !ok {
		return false, nil
	}

	opened, err := reader.Open(ctx, manifest.ObjectDigest)
	if err != nil {
		return true, fmt.Errorf("open RDF bundle %s: %w", manifest.ObjectDigest, err)
	}
	defer opened.Close()

	content, err := io.ReadAll(opened)
	if err != nil {
		return true, fmt.Errorf("read RDF bundle %s: %w", manifest.ObjectDigest, err)
	}

	statements, annotations, err := parseRDFBundle(string(content))
	if err != nil {
		return true, fmt.Errorf("parse RDF bundle %s: %w", manifest.ObjectDigest, err)
	}

	for index, statement := range statements {
		err := insertRDFStatement(
			ctx,
			transaction,
			manifest.ObjectDigest,
			statement,
			index,
		)
		if err != nil {
			return true, err
		}
	}

	for _, annotation := range annotations {
		err := insertRDFAnnotation(
			ctx,
			transaction,
			manifest.ObjectDigest,
			annotation,
		)
		if err != nil {
			return true, err
		}
	}

	return true, nil
}

func objectHadRDFRowsTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
) (bool, error) {
	var exists bool

	err := transaction.QueryRow(
		ctx,
		`SELECT EXISTS (
		   SELECT 1 FROM query_rdf_statements WHERE source_digest = $1
		   UNION ALL
		   SELECT 1 FROM query_rdf_statement_annotations WHERE source_digest = $1
		 )`,
		digest,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check stale RDF projection rows: %w", err)
	}

	return exists, nil
}

func rdfSubjectsForSourceTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
) ([]string, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT DISTINCT subj.term_value
		   FROM query_rdf_statements s
		   JOIN query_rdf_terms subj ON subj.term_id = s.subject_term_id
		  WHERE s.source_digest = $1`,
		digest,
	)
	if err != nil {
		return nil, fmt.Errorf("query RDF source subjects: %w", err)
	}
	defer rows.Close()

	subjects := []string{}

	for rows.Next() {
		var subject string

		err = rows.Scan(&subject)
		if err != nil {
			return nil, fmt.Errorf("scan RDF source subject: %w", err)
		}

		subjects = append(subjects, subject)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate RDF source subjects: %w", err)
	}

	return subjects, nil
}

func insertRDFStatement(
	ctx context.Context,
	transaction pgx.Tx,
	sourceDigest contracts.ObjectDigest,
	statement rdfStatement,
	order int,
) error {
	subjectID, err := upsertRDFTerm(ctx, transaction, statement.subject)
	if err != nil {
		return err
	}

	predicateID, err := upsertRDFTerm(ctx, transaction, statement.predicate)
	if err != nil {
		return err
	}

	objectID, err := upsertRDFTerm(ctx, transaction, statement.object)
	if err != nil {
		return err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO query_rdf_statements(
		   statement_hash, source_digest, subject_term_id, predicate_term_id,
		   object_term_id, statement_order, projected_at
		 ) VALUES($1,$2,$3,$4,$5,$6,now())
		 ON CONFLICT(source_digest, statement_hash) DO UPDATE SET
		   subject_term_id = excluded.subject_term_id,
		   predicate_term_id = excluded.predicate_term_id,
		   object_term_id = excluded.object_term_id,
		   statement_order = excluded.statement_order,
		   projected_at = now()`,
		statement.hash,
		sourceDigest,
		subjectID,
		predicateID,
		objectID,
		order,
	)
	if err != nil {
		return fmt.Errorf("insert RDF statement projection: %w", err)
	}

	return nil
}

func insertRDFAnnotation(
	ctx context.Context,
	transaction pgx.Tx,
	sourceDigest contracts.ObjectDigest,
	annotation rdfAnnotation,
) error {
	predicateID, err := upsertRDFTerm(ctx, transaction, annotation.predicate)
	if err != nil {
		return err
	}

	objectID, err := upsertRDFTerm(ctx, transaction, annotation.object)
	if err != nil {
		return err
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO query_rdf_statement_annotations(
		   source_digest, statement_hash, annotation_predicate_term_id,
		   annotation_object_term_id, projected_at
		 ) VALUES($1,$2,$3,$4,now())
		 ON CONFLICT DO NOTHING`,
		sourceDigest,
		annotation.hash,
		predicateID,
		objectID,
	)
	if err != nil {
		return fmt.Errorf("insert RDF statement annotation projection: %w", err)
	}

	return nil
}

func upsertRDFTerm(
	ctx context.Context,
	transaction pgx.Tx,
	term rdfTerm,
) (int64, error) {
	var termID int64

	err := transaction.QueryRow(
		ctx,
		`INSERT INTO query_rdf_terms(term_kind, term_value, language, datatype)
		 VALUES($1,$2,$3,$4)
		 ON CONFLICT(term_kind, term_value, language, datatype) DO UPDATE SET
		   term_value = excluded.term_value
		 RETURNING term_id`,
		term.kind,
		term.value,
		term.language,
		term.datatype,
	).Scan(&termID)
	if err != nil {
		return 0, fmt.Errorf("upsert RDF term: %w", err)
	}

	return termID, nil
}

func parseRDFBundle(content string) ([]rdfStatement, []rdfAnnotation, error) {
	parser := rdfBundleParser{
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

func (parser *rdfBundleParser) consume(raw string) {
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

func (parser *rdfBundleParser) consumeAnnotationContinuation(
	line string,
	indented bool,
) bool {
	if parser.currentAnnotation == "" || !indented {
		return false
	}

	next := parseRDFAnnotationLine(parser.currentAnnotation, line, parser.prefixes)
	parser.annotations = append(parser.annotations, next...)

	if strings.HasSuffix(line, ".") {
		parser.currentAnnotation = ""
	}

	return true
}

func (parser *rdfBundleParser) consumeRDFStar(line string) bool {
	if !strings.HasPrefix(line, "<<") {
		return false
	}

	annotation, found := parseRDFStarAnnotation(line, parser.prefixes)
	if found {
		parser.statements = append(parser.statements, annotation.statement)
		parser.annotations = append(parser.annotations, annotation)

		return true
	}

	statement, found := parseRDFStarSubject(line, parser.prefixes)
	if found {
		parser.statements = append(parser.statements, statement)
		parser.currentAnnotation = statement.hash
	}

	return true
}

func (parser *rdfBundleParser) consumeObjectContinuation(line string) bool {
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

func (parser *rdfBundleParser) consumeIndentedPredicate(
	line string,
	indented bool,
) bool {
	if parser.currentSubject.value == "" || !indented {
		return false
	}

	return parser.consumePredicate(line)
}

func (parser *rdfBundleParser) consumeSubject(line string) bool {
	subject, rest, found := parseSubjectLine(line, parser.prefixes)
	if !found {
		return false
	}

	parser.currentSubject = subject
	parser.currentPredicate = rdfTerm{}
	parser.continuingObjects = false

	if strings.TrimSpace(rest) == "" {
		return true
	}

	parser.consumePredicate(rest)

	return true
}

func (parser *rdfBundleParser) consumePredicate(line string) bool {
	if parser.currentSubject.value == "" {
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

func parseSubjectLine(
	line string,
	prefixes map[string]string,
) (rdfTerm, string, bool) {
	first, rest := splitFirstToken(line)
	if first == "" {
		return rdfTerm{}, "", false
	}

	if !looksLikeRDFTerm(first) {
		return rdfTerm{}, "", false
	}

	subject, ok := parseRDFTerm(first, prefixes)
	if !ok || subject.kind == "literal" {
		return rdfTerm{}, "", false
	}

	return subject, rest, true
}

func parsePredicateObjectLine(
	subject rdfTerm,
	line string,
	prefixes map[string]string,
) (rdfTerm, []rdfStatement, bool) {
	predicateToken, rest := splitFirstToken(line)

	predicate, parsed := parsePredicateTerm(predicateToken, prefixes)
	if !parsed {
		return rdfTerm{}, nil, false
	}

	statements, parsed := parseObjectList(subject, predicate, rest, prefixes)
	if !parsed {
		return rdfTerm{}, nil, false
	}

	return predicate, statements, true
}

func parseObjectList(
	subject rdfTerm,
	predicate rdfTerm,
	line string,
	prefixes map[string]string,
) ([]rdfStatement, bool) {
	line = strings.TrimSpace(line)
	line = strings.TrimSuffix(line, ";")
	line = strings.TrimSuffix(line, ".")
	items := splitRDFObjects(line)
	statements := []rdfStatement{}

	for _, item := range items {
		object, ok := parseRDFTerm(item, prefixes)
		if !ok {
			continue
		}

		statement := rdfStatement{
			subject:   subject,
			predicate: predicate,
			object:    object,
		}
		statement.hash = rdfStatementHash(statement)
		statements = append(statements, statement)
	}

	return statements, len(statements) > 0
}

func parseRDFStarAnnotation(
	line string,
	prefixes map[string]string,
) (rdfAnnotation, bool) {
	statement, rest, cutFound := parseRDFStarStatement(line, prefixes)
	if !cutFound {
		return rdfAnnotation{}, false
	}

	annotationPredicateToken, annotationObjectText := splitFirstToken(rest)

	annotationPredicate, parsed := parsePredicateTerm(annotationPredicateToken, prefixes)
	if !parsed {
		return rdfAnnotation{}, false
	}

	annotationObject, parsed := parseRDFTerm(annotationObjectText, prefixes)
	if !parsed {
		return rdfAnnotation{}, false
	}

	return rdfAnnotation{
		statement: statement,
		hash:      rdfStatementHash(statement),
		predicate: annotationPredicate,
		object:    annotationObject,
	}, true
}

func parseRDFStarSubject(line string, prefixes map[string]string) (rdfStatement, bool) {
	statement, rest, cutFound := parseRDFStarStatement(line, prefixes)
	if !cutFound || strings.TrimSpace(strings.TrimSuffix(rest, ";")) != "" {
		return rdfStatement{}, false
	}

	return statement, true
}

func parseRDFStarStatement(
	line string,
	prefixes map[string]string,
) (rdfStatement, string, bool) {
	inner, rest, cutFound := strings.Cut(strings.TrimPrefix(line, "<<"), ">>")
	if !cutFound {
		return rdfStatement{}, "", false
	}

	subjectToken, restInner := splitFirstToken(inner)
	predicateToken, objectText := splitFirstToken(restInner)

	subject, parsed := parseRDFTerm(subjectToken, prefixes)
	if !parsed {
		return rdfStatement{}, "", false
	}

	predicate, parsed := parsePredicateTerm(predicateToken, prefixes)
	if !parsed {
		return rdfStatement{}, "", false
	}

	object, parsed := parseRDFTerm(objectText, prefixes)
	if !parsed {
		return rdfStatement{}, "", false
	}

	statement := rdfStatement{
		subject:   subject,
		predicate: predicate,
		object:    object,
	}
	statement.hash = rdfStatementHash(statement)

	return statement, rest, true
}

func parseRDFAnnotationLine(
	statementHash string,
	line string,
	prefixes map[string]string,
) []rdfAnnotation {
	predicateToken, rest := splitFirstToken(line)

	predicate, ok := parsePredicateTerm(predicateToken, prefixes)
	if !ok {
		return nil
	}

	rest = strings.TrimSuffix(strings.TrimSuffix(strings.TrimSpace(rest), ";"), ".")
	annotations := []rdfAnnotation{}

	for _, item := range splitRDFObjects(rest) {
		object, ok := parseRDFTerm(item, prefixes)
		if !ok {
			continue
		}

		annotations = append(annotations, rdfAnnotation{
			hash:      statementHash,
			predicate: predicate,
			object:    object,
		})
	}

	return annotations
}

func parsePredicateTerm(token string, prefixes map[string]string) (rdfTerm, bool) {
	if token == "a" {
		return rdfTerm{kind: "iri", value: rdfTypePredicate}, true
	}

	return parseRDFTerm(token, prefixes)
}

func parseRDFTerm(token string, prefixes map[string]string) (rdfTerm, bool) {
	token = cleanRDFToken(token)
	if token == "" || strings.HasPrefix(token, "[") {
		return rdfTerm{}, false
	}

	if strings.HasPrefix(token, "<") && strings.Contains(token, ">") {
		value, _, _ := strings.Cut(strings.TrimPrefix(token, "<"), ">")

		return rdfTerm{kind: "iri", value: value}, true
	}

	if strings.HasPrefix(token, "\"") {
		return parseLiteralTerm(token, prefixes), true
	}

	if prefix, suffix, ok := strings.Cut(token, ":"); ok {
		if base := prefixes[prefix]; base != "" {
			return rdfTerm{kind: "iri", value: base + suffix}, true
		}
	}

	return rdfTerm{}, false
}

func parseLiteralTerm(token string, prefixes map[string]string) rdfTerm {
	value, suffix := splitLiteralValue(token)

	term := rdfTerm{kind: "literal", value: value}
	if after, ok := strings.CutPrefix(suffix, "@"); ok {
		term.language = after
	}

	if after, ok := strings.CutPrefix(suffix, "^^"); ok {
		term.datatype = parseDatatypeIRI(after, prefixes)
	}

	return term
}

func parseDatatypeIRI(token string, prefixes map[string]string) string {
	term, ok := parseRDFTerm(token, prefixes)
	if ok && term.kind == "iri" {
		return term.value
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

func cleanRDFToken(token string) string {
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

func splitRDFObjects(line string) []string {
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

func looksLikeRDFTerm(token string) bool {
	return strings.HasPrefix(token, "<") ||
		strings.HasPrefix(token, "_:") ||
		strings.Contains(token, ":")
}

func rdfStatementHash(statement rdfStatement) string {
	parts := []string{
		statement.subject.kind,
		statement.subject.value,
		statement.predicate.value,
		statement.object.kind,
		statement.object.value,
		statement.object.language,
		statement.object.datatype,
	}
	sum := sha256.Sum256([]byte(strings.Join(parts, "\x00")))

	return hex.EncodeToString(sum[:])
}

func refreshContactProjectionTx(ctx context.Context, transaction pgx.Tx) error {
	for _, table := range []string{
		"query_contact_identity_bindings",
		"query_contact_facts",
		"query_contact_rollups",
	} {
		_, err := transaction.Exec(ctx, "DELETE FROM "+table)
		if err != nil {
			return fmt.Errorf("clear %s: %w", table, err)
		}
	}

	statements, err := contactProjectionStatements(ctx, transaction)
	if err != nil {
		return err
	}

	annotations, err := contactProjectionAnnotations(ctx, transaction)
	if err != nil {
		return err
	}

	contacts := contactSubjects(statements)

	facts := contactFactsFromStatements(statements, annotations, contacts)
	for _, fact := range facts {
		err := insertContactFact(ctx, transaction, fact)
		if err != nil {
			return err
		}
	}

	return refreshContactRollupsTx(ctx, transaction)
}

func refreshContactProjectionForContactsTx(
	ctx context.Context,
	transaction pgx.Tx,
	contacts []string,
) error {
	contacts = uniqueNonEmptyStrings(contacts)
	if len(contacts) == 0 {
		return nil
	}

	for _, table := range []string{
		"query_contact_identity_bindings",
		"query_contact_facts",
		"query_contact_rollups",
	} {
		_, err := transaction.Exec(
			ctx,
			"DELETE FROM "+table+" WHERE contact_id = ANY($1)",
			contacts,
		)
		if err != nil {
			return fmt.Errorf("clear scoped %s: %w", table, err)
		}
	}

	statements, err := contactProjectionStatementsForContacts(ctx, transaction, contacts)
	if err != nil {
		return err
	}

	annotations, err := contactProjectionAnnotationsForContacts(ctx, transaction, contacts)
	if err != nil {
		return err
	}

	contactSet := contactSubjects(statements)

	facts := contactFactsFromStatements(statements, annotations, contactSet)
	for _, fact := range facts {
		err := insertContactFact(ctx, transaction, fact)
		if err != nil {
			return err
		}
	}

	return refreshContactRollupsForContactsTx(ctx, transaction, contacts)
}

func refreshContactProjection(ctx context.Context, beginner transactionBeginner) error {
	transaction, err := beginner.BeginTx(ctx, pgx.TxOptions{})
	if err != nil {
		return fmt.Errorf("begin contact projection refresh: %w", err)
	}

	defer rollbackProjectionTx(ctx, transaction)

	err = refreshContactProjectionTx(ctx, transaction)
	if err != nil {
		return err
	}

	err = transaction.Commit(ctx)
	if err != nil {
		return fmt.Errorf("commit contact projection refresh: %w", err)
	}

	return nil
}

func rollbackProjectionTx(ctx context.Context, transaction pgx.Tx) {
	err := transaction.Rollback(ctx)
	if err != nil && !errors.Is(err, pgx.ErrTxClosed) {
		observability.Logger(ctx).Warn(
			"rollback contact projection refresh failed",
			"error",
			err,
		)
	}
}

func contactProjectionStatements(
	ctx context.Context,
	transaction pgx.Tx,
) ([]contactProjectionStatement, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT s.source_digest, s.statement_hash,
		        subj.term_value, pred.term_value, obj.term_value, obj.term_kind
		   FROM query_rdf_statements s
		   JOIN query_rdf_terms subj ON subj.term_id = s.subject_term_id
		   JOIN query_rdf_terms pred ON pred.term_id = s.predicate_term_id
		   JOIN query_rdf_terms obj ON obj.term_id = s.object_term_id
		  ORDER BY s.statement_order, s.statement_hash`,
	)
	if err != nil {
		return nil, fmt.Errorf("query RDF statements for contact projection: %w", err)
	}
	defer rows.Close()

	return scanContactProjectionStatements(rows)
}

func scanContactProjectionStatements(
	rows pgx.Rows,
) ([]contactProjectionStatement, error) {
	statements := []contactProjectionStatement{}

	for rows.Next() {
		var statement contactProjectionStatement

		err := rows.Scan(
			&statement.sourceDigest,
			&statement.statementHash,
			&statement.subject,
			&statement.predicate,
			&statement.object,
			&statement.objectKind,
		)
		if err != nil {
			return nil, fmt.Errorf("scan RDF statement for contact projection: %w", err)
		}

		statements = append(statements, statement)
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate RDF statements for contact projection: %w", err)
	}

	return statements, nil
}

func contactProjectionStatementsForContacts(
	ctx context.Context,
	transaction pgx.Tx,
	contacts []string,
) ([]contactProjectionStatement, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT s.source_digest, s.statement_hash,
		subj.term_value, pred.term_value, obj.term_value, obj.term_kind
		FROM query_rdf_statements s
		JOIN query_rdf_terms subj ON subj.term_id = s.subject_term_id
		JOIN query_rdf_terms pred ON pred.term_id = s.predicate_term_id
		JOIN query_rdf_terms obj ON obj.term_id = s.object_term_id
		WHERE subj.term_value = ANY($1)
		ORDER BY s.statement_order, s.statement_hash`,
		contacts,
	)
	if err != nil {
		return nil, fmt.Errorf("query scoped RDF statements for contact projection: %w", err)
	}
	defer rows.Close()

	return scanContactProjectionStatements(rows)
}

func contactProjectionAnnotations(
	ctx context.Context,
	transaction pgx.Tx,
) (map[sourceStatementKey]map[string]string, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT a.source_digest, a.statement_hash, pred.term_value, obj.term_value
		   FROM query_rdf_statement_annotations a
		   JOIN query_rdf_terms pred ON pred.term_id = a.annotation_predicate_term_id
		   JOIN query_rdf_terms obj ON obj.term_id = a.annotation_object_term_id`,
	)
	if err != nil {
		return nil, fmt.Errorf("query RDF annotations for contact projection: %w", err)
	}
	defer rows.Close()

	return scanContactProjectionAnnotations(rows)
}

func scanContactProjectionAnnotations(
	rows pgx.Rows,
) (map[sourceStatementKey]map[string]string, error) {
	annotations := map[sourceStatementKey]map[string]string{}

	for rows.Next() {
		var sourceDigest contracts.ObjectDigest

		var hash, predicate, object string

		err := rows.Scan(&sourceDigest, &hash, &predicate, &object)
		if err != nil {
			return nil, fmt.Errorf("scan RDF annotation for contact projection: %w", err)
		}

		key := sourceStatementKey{
			sourceDigest:  sourceDigest,
			statementHash: hash,
		}
		if annotations[key] == nil {
			annotations[key] = map[string]string{}
		}

		annotations[key][predicate] = object
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate RDF annotations for contact projection: %w", err)
	}

	return annotations, nil
}

func contactProjectionAnnotationsForContacts(
	ctx context.Context,
	transaction pgx.Tx,
	contacts []string,
) (map[sourceStatementKey]map[string]string, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT a.source_digest, a.statement_hash, pred.term_value, obj.term_value
		   FROM query_rdf_statement_annotations a
		   JOIN query_rdf_statements s
		     ON s.source_digest = a.source_digest
		    AND s.statement_hash = a.statement_hash
		   JOIN query_rdf_terms subj ON subj.term_id = s.subject_term_id
		   JOIN query_rdf_terms pred ON pred.term_id = a.annotation_predicate_term_id
		   JOIN query_rdf_terms obj ON obj.term_id = a.annotation_object_term_id
		  WHERE subj.term_value = ANY($1)`,
		contacts,
	)
	if err != nil {
		return nil, fmt.Errorf("query scoped RDF annotations for contact projection: %w", err)
	}
	defer rows.Close()

	return scanContactProjectionAnnotations(rows)
}

func contactSubjects(statements []contactProjectionStatement) map[string]bool {
	contacts := map[string]bool{}

	for _, statement := range statements {
		if statement.predicate != rdfTypePredicate {
			continue
		}

		if contactTypeObject(statement.object) {
			contacts[statement.subject] = true
		}
	}

	return contacts
}

func contactTypeObject(value string) bool {
	return value == "http://xmlns.com/foaf/0.1/Person" ||
		value == "https://schema.org/Person" ||
		value == "http://www.w3.org/2000/10/swap/pim/gedcom#Individual"
}

func contactFactsFromStatements(
	statements []contactProjectionStatement,
	annotations map[sourceStatementKey]map[string]string,
	contacts map[string]bool,
) []contactProjectionFact {
	facts := []contactProjectionFact{}

	for _, statement := range statements {
		if !contacts[statement.subject] {
			continue
		}

		factKind, historical, ok := contactFactKind(statement.predicate)
		if !ok {
			continue
		}

		value := contactFactValue(statement.object, statement.objectKind, factKind)
		if strings.TrimSpace(value) == "" {
			continue
		}

		fact := contactProjectionFact{
			sourceDigest:  statement.sourceDigest,
			statementHash: statement.statementHash,
			contactID:     statement.subject,
			factKind:      factKind,
			value:         value,
			predicate:     statement.predicate,
			historical:    historical,
		}

		key := sourceStatementKey{
			sourceDigest:  statement.sourceDigest,
			statementHash: statement.statementHash,
		}
		for predicate, object := range annotations[key] {
			switch {
			case strings.HasSuffix(predicate, "hasBeginning") ||
				strings.HasSuffix(predicate, "validFrom"):
				fact.validFrom = object
			case strings.HasSuffix(predicate, "hasEnd") ||
				strings.HasSuffix(predicate, "validUntil"):
				fact.validUntil = object
			}
		}

		facts = append(facts, fact)
	}

	return facts
}

func contactFactKind(predicate string) (string, bool, bool) {
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

	kind, historical, found := patrickAudleyContactFactKind(predicate)
	if found {
		return kind, historical, true
	}

	kind, found = schemaContactFactKind(predicate)
	if found {
		return kind, false, true
	}

	return "", false, false
}

func relationshipContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://purl.org/vocab/relationship/childOf",
		"http://purl.org/vocab/relationship/parentOf",
		"http://purl.org/vocab/relationship/spouseOf":
		return contactFactRelationship, true
	default:
		return "", false
	}
}

func owlContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://www.w3.org/2002/07/owl#sameAs",
		"http://www.w3.org/2004/02/skos/core#exactMatch":
		return contactFactIdentifier, true
	default:
		return "", false
	}
}

func vcardContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://www.w3.org/2006/vcard/ns#fn":
		return contactFactName, true
	case "http://www.w3.org/2006/vcard/ns#hasAddress":
		return contactFactAddress, true
	case "http://www.w3.org/2006/vcard/ns#hasEmail":
		return contactFactEmail, true
	case "http://www.w3.org/2006/vcard/ns#hasTelephone":
		return contactFactPhone, true
	case "http://www.w3.org/2006/vcard/ns#hasURL":
		return contactFactURL, true
	case "http://www.w3.org/2006/vcard/ns#nickname":
		return contactFactAlias, true
	default:
		return "", false
	}
}

func orgContactFactKind(predicate string) (string, bool) {
	if predicate == "http://www.w3.org/ns/org#member" {
		return contactFactAffiliation, true
	}

	return "", false
}

func foafContactFactKind(predicate string) (string, bool) {
	switch predicate {
	case "http://xmlns.com/foaf/0.1/account":
		return contactFactAccount, true
	case "http://xmlns.com/foaf/0.1/homepage":
		return contactFactURL, true
	case "http://xmlns.com/foaf/0.1/knows":
		return contactFactRelationship, true
	case "http://xmlns.com/foaf/0.1/mbox":
		return contactFactEmail, true
	case "http://xmlns.com/foaf/0.1/name":
		return contactFactName, true
	case "http://xmlns.com/foaf/0.1/nick":
		return contactFactAlias, true
	case "http://xmlns.com/foaf/0.1/phone":
		return contactFactPhone, true
	case "http://xmlns.com/foaf/0.1/title":
		return contactFactTitle, true
	default:
		return "", false
	}
}

func patrickAudleyContactFactKind(predicate string) (string, bool, bool) {
	switch predicate {
	case "https://patrickaudley.com/lod#emailIdentity":
		return contactFactEmail, false, true
	case "https://patrickaudley.com/lod#historicalEmail":
		return contactFactEmail, true, true
	default:
		return "", false, false
	}
}

func schemaContactFactKind(predicate string) (string, bool) {
	if kind, found := schemaContactFactKindEarly(predicate); found {
		return kind, true
	}

	return schemaContactFactKindLate(predicate)
}

func schemaContactFactKindEarly(predicate string) (string, bool) {
	switch predicate {
	case "https://schema.org/address":
		return contactFactAddress, true
	case "https://schema.org/affiliation":
		return contactFactAffiliation, true
	case "https://schema.org/alternateName":
		return contactFactAlias, true
	case "https://schema.org/email":
		return contactFactEmail, true
	case "https://schema.org/identifier":
		return contactFactIdentifier, true
	case "https://schema.org/jobTitle":
		return contactFactTitle, true
	default:
		return "", false
	}
}

func schemaContactFactKindLate(predicate string) (string, bool) {
	switch predicate {
	case "https://schema.org/knows":
		return contactFactRelationship, true
	case "https://schema.org/memberOf":
		return contactFactAffiliation, true
	case "https://schema.org/name":
		return contactFactName, true
	case "https://schema.org/sameAs":
		return contactFactIdentifier, true
	case "https://schema.org/telephone":
		return contactFactPhone, true
	case "https://schema.org/url":
		return contactFactURL, true
	case "https://schema.org/worksFor":
		return contactFactAffiliation, true
	default:
		return "", false
	}
}

func contactFactValue(value, objectKind, factKind string) string {
	if factKind == contactFactEmail {
		return normalizeContactEmail(value)
	}

	if objectKind == "literal" {
		return strings.TrimSpace(value)
	}

	return strings.TrimSpace(value)
}

func insertContactFact(
	ctx context.Context,
	transaction pgx.Tx,
	fact contactProjectionFact,
) error {
	metadata, err := json.Marshal(map[string]any{
		"historical": fact.historical,
	})
	if err != nil {
		return fmt.Errorf("encode contact fact metadata: %w", err)
	}

	_, err = transaction.Exec(
		ctx,
		`INSERT INTO query_contact_facts(
		   contact_id, fact_kind, value, value_hash, predicate, source_digest,
		   statement_hash, valid_from, valid_until, historical, metadata_json
		 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
		 ON CONFLICT(contact_id, fact_kind, value_hash, statement_hash) DO UPDATE SET
		   value = excluded.value,
		   predicate = excluded.predicate,
		   source_digest = CASE
		     WHEN excluded.valid_from <> '' OR excluded.valid_until <> ''
		     THEN excluded.source_digest
		     ELSE query_contact_facts.source_digest
		   END,
		   valid_from = COALESCE(NULLIF(excluded.valid_from, ''), query_contact_facts.valid_from),
		   valid_until = COALESCE(NULLIF(excluded.valid_until, ''), query_contact_facts.valid_until),
		   historical = excluded.historical,
		   metadata_json = excluded.metadata_json`,
		fact.contactID,
		fact.factKind,
		fact.value,
		hashString(fact.value),
		fact.predicate,
		fact.sourceDigest,
		fact.statementHash,
		fact.validFrom,
		fact.validUntil,
		fact.historical,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("insert contact fact projection: %w", err)
	}

	if fact.factKind != contactFactEmail {
		return nil
	}

	return insertContactIdentityBinding(ctx, transaction, fact)
}

func insertContactIdentityBinding(
	ctx context.Context,
	transaction pgx.Tx,
	fact contactProjectionFact,
) error {
	token := normalizeContactEmail(fact.value)
	if token == "" {
		return nil
	}

	_, err := transaction.Exec(
		ctx,
		`INSERT INTO query_contact_identity_bindings(
		   token_hash, token, contact_id, statement_hash, valid_from,
		   valid_until, source_digest
		 ) VALUES($1,$2,$3,$4,$5,$6,$7)
		 ON CONFLICT(token_hash, contact_id, statement_hash) DO UPDATE SET
		   token = excluded.token,
		   valid_from = COALESCE(
		     NULLIF(excluded.valid_from, ''),
		     query_contact_identity_bindings.valid_from
		   ),
		   valid_until = COALESCE(
		     NULLIF(excluded.valid_until, ''),
		     query_contact_identity_bindings.valid_until
		   ),
		   source_digest = CASE
		     WHEN excluded.valid_from <> '' OR excluded.valid_until <> ''
		     THEN excluded.source_digest
		     ELSE query_contact_identity_bindings.source_digest
		   END`,
		hashString(token),
		token,
		fact.contactID,
		fact.statementHash,
		fact.validFrom,
		fact.validUntil,
		fact.sourceDigest,
	)
	if err != nil {
		return fmt.Errorf("insert contact identity binding projection: %w", err)
	}

	return nil
}

func refreshContactRollupsTx(ctx context.Context, transaction pgx.Tx) error {
	_, err := transaction.Exec(
		ctx,
		`INSERT INTO query_contact_rollups(
		   contact_id, display_name, primary_email, fact_count, search_text, updated_at
		 )
		 SELECT f.contact_id,
		        COALESCE(
		          min(f.value) FILTER (WHERE f.fact_kind = 'name'),
		          min(f.value) FILTER (WHERE f.fact_kind = 'alias'),
		          f.contact_id
		        ) AS display_name,
		        COALESCE(
		          min(f.value) FILTER (
		            WHERE f.fact_kind = 'email' AND f.historical = false
		          ),
		          min(f.value) FILTER (WHERE f.fact_kind = 'email'),
		          ''
		        ) AS primary_email,
		        count(*)::integer AS fact_count,
		        string_agg(f.value, ' ' ORDER BY f.fact_kind, f.value) AS search_text,
		        now()
		   FROM query_contact_facts f
		  GROUP BY f.contact_id
		 ON CONFLICT(contact_id) DO UPDATE SET
		   display_name = excluded.display_name,
		   primary_email = excluded.primary_email,
		   fact_count = excluded.fact_count,
		   search_text = excluded.search_text,
		   updated_at = now()`,
	)
	if err != nil {
		return fmt.Errorf("refresh contact rollups: %w", err)
	}

	return nil
}

func refreshContactRollupsForContactsTx(
	ctx context.Context,
	transaction pgx.Tx,
	contacts []string,
) error {
	_, err := transaction.Exec(
		ctx,
		`INSERT INTO query_contact_rollups(
		contact_id, display_name, primary_email, fact_count, search_text, updated_at
		)
		SELECT f.contact_id,
		COALESCE(
		min(f.value) FILTER (WHERE f.fact_kind = 'name'),
		min(f.value) FILTER (WHERE f.fact_kind = 'alias'),
		f.contact_id
		) AS display_name,
		COALESCE(
		min(f.value) FILTER (
		WHERE f.fact_kind = 'email' AND f.historical = false
		),
		min(f.value) FILTER (WHERE f.fact_kind = 'email'),
		''
		) AS primary_email,
		count(*)::integer AS fact_count,
		string_agg(f.value, ' ' ORDER BY f.fact_kind, f.value) AS search_text,
		now()
		FROM query_contact_facts f
		WHERE f.contact_id = ANY($1)
		GROUP BY f.contact_id
		ON CONFLICT(contact_id) DO UPDATE SET
		display_name = excluded.display_name,
		primary_email = excluded.primary_email,
		fact_count = excluded.fact_count,
		search_text = excluded.search_text,
		updated_at = now()`,
		contacts,
	)
	if err != nil {
		return fmt.Errorf("refresh scoped contact rollups: %w", err)
	}

	return nil
}

func (index *Index) ContactAggregate(
	ctx context.Context,
	request contracts.ContactAggregateRequest,
) (contracts.ContactAggregate, error) {
	contactID := strings.TrimSpace(request.ContactID)

	var aggregate contracts.ContactAggregate

	err := index.pool.QueryRow(
		ctx,
		`SELECT contact_id, display_name, primary_email, fact_count
		   FROM query_contact_rollups
		  WHERE contact_id = $1`,
		contactID,
	).Scan(
		&aggregate.ContactID,
		&aggregate.DisplayName,
		&aggregate.PrimaryEmail,
		&aggregate.FactCount,
	)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return contracts.ContactAggregate{
				ContactID:     contactID,
				Facts:         []contracts.ContactFact{},
				SchemaVersion: contracts.SchemaVersionPhase00,
			}, nil
		}

		return contracts.ContactAggregate{}, fmt.Errorf("read contact aggregate: %w", err)
	}

	rows, err := index.pool.Query(
		ctx,
		`SELECT source_digest, statement_hash, fact_kind, value, predicate,
		        valid_from, valid_until, historical
		   FROM query_contact_facts
		  WHERE contact_id = $1
		  ORDER BY fact_kind, value, statement_hash`,
		contactID,
	)
	if err != nil {
		return contracts.ContactAggregate{}, fmt.Errorf("query contact facts: %w", err)
	}
	defer rows.Close()

	for rows.Next() {
		var fact contracts.ContactFact

		fact.ContactID = contactID

		err = rows.Scan(
			&fact.SourceDigest,
			&fact.StatementHash,
			&fact.FactKind,
			&fact.Value,
			&fact.Predicate,
			&fact.ValidFrom,
			&fact.ValidUntil,
			&fact.Historical,
		)
		if err != nil {
			return contracts.ContactAggregate{}, fmt.Errorf("scan contact fact: %w", err)
		}

		aggregate.Facts = append(aggregate.Facts, fact)
	}

	err = rows.Err()
	if err != nil {
		return contracts.ContactAggregate{}, fmt.Errorf("iterate contact facts: %w", err)
	}

	aggregate.SchemaVersion = contracts.SchemaVersionPhase00

	return aggregate, nil
}

func (index *Index) ContactSearch(
	ctx context.Context,
	request contracts.ContactSearchRequest,
) (contracts.ContactSearchResponse, error) {
	args := []any{}
	where := []string{"true"}

	if queryText := strings.TrimSpace(request.Query); queryText != "" {
		args = append(args, queryText)
		where = append(
			where,
			fmt.Sprintf("search_tsv @@ websearch_to_tsquery('simple', $%d)", len(args)),
		)
	}

	limit := normalizedLimit(request.Limit)

	offset := max(request.Offset, 0)
	whereSQL := strings.Join(where, " AND ")

	total, err := countContactSearch(ctx, index.pool, whereSQL, args)
	if err != nil {
		return contracts.ContactSearchResponse{}, err
	}

	args = append(args, limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT contact_id, display_name, primary_email, fact_count
		   FROM query_contact_rollups
		  WHERE %s
		  ORDER BY display_name, contact_id
		  LIMIT $%d OFFSET $%d`,
		whereSQL,
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.ContactSearchResponse{}, fmt.Errorf("search contacts: %w", err)
	}
	defer rows.Close()

	results := []contracts.ContactSearchResult{}

	for rows.Next() {
		var result contracts.ContactSearchResult

		err = rows.Scan(
			&result.ContactID,
			&result.DisplayName,
			&result.PrimaryEmail,
			&result.FactCount,
		)
		if err != nil {
			return contracts.ContactSearchResponse{}, fmt.Errorf("scan contact search: %w", err)
		}

		result.Score = 1
		results = append(results, result)
	}

	err = rows.Err()
	if err != nil {
		return contracts.ContactSearchResponse{}, fmt.Errorf(
			"iterate contact search: %w",
			err,
		)
	}

	response := contracts.ContactSearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}

	return response, nil
}

func countContactSearch(
	ctx context.Context,
	counter contactSearchCounter,
	whereSQL string,
	args []any,
) (int, error) {
	total := 0

	err := counter.QueryRow(
		ctx,
		`SELECT count(*)::integer FROM query_contact_rollups WHERE `+whereSQL,
		args...,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count contacts: %w", err)
	}

	return total, nil
}

func (index *Index) ResolveContactIdentity(
	ctx context.Context,
	request contracts.ContactIdentityResolveRequest,
) (contracts.ContactIdentityResolveResponse, error) {
	token := normalizeContactEmail(request.Identity)
	if token == "" {
		return contracts.ContactIdentityResolveResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
		}, nil
	}

	rows, err := index.pool.Query(
		ctx,
		`SELECT DISTINCT contact_id
		   FROM query_contact_identity_bindings
		  WHERE token_hash = $1
		  ORDER BY contact_id`,
		hashString(token),
	)
	if err != nil {
		return contracts.ContactIdentityResolveResponse{}, fmt.Errorf(
			"resolve contact identity: %w",
			err,
		)
	}
	defer rows.Close()

	response := contracts.ContactIdentityResolveResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
	}

	for rows.Next() {
		var contactID string

		err = rows.Scan(&contactID)
		if err != nil {
			return contracts.ContactIdentityResolveResponse{}, fmt.Errorf(
				"scan contact identity resolution: %w",
				err,
			)
		}

		response.ContactIDs = append(response.ContactIDs, contactID)
	}

	err = rows.Err()
	if err != nil {
		return contracts.ContactIdentityResolveResponse{}, fmt.Errorf(
			"iterate contact identity resolution: %w",
			err,
		)
	}

	return response, nil
}

func normalizeContactEmail(value string) string {
	value = strings.TrimSpace(value)

	value = strings.TrimPrefix(value, "mailto:")

	address, err := mail.ParseAddress(value)
	if err == nil {
		value = address.Address
	}

	return strings.ToLower(strings.TrimSpace(value))
}

func uniqueNonEmptyStrings(values []string) []string {
	seen := map[string]bool{}
	unique := []string{}

	for _, value := range values {
		value = strings.TrimSpace(value)
		if value == "" || seen[value] {
			continue
		}

		seen[value] = true
		unique = append(unique, value)
	}

	return unique
}

func hashString(value string) string {
	sum := sha256.Sum256([]byte(value))

	return hex.EncodeToString(sum[:])
}
