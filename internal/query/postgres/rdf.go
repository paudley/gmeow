// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"bufio"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"
	"unicode"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/query"
)

const (
	prefixFieldCount      = 3
	rdfScannerBufferSize  = 4096
	rdfScannerMaxCapacity = 1024 * 1024
	rdfTypePredicate      = "http://www.w3.org/1999/02/22-rdf-syntax-ns#type"
	splitQuoteOffset      = 2
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

type transactionBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type contactSearchCounter interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
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

	facts := contactentity.FactsFromStatements(statements, annotations)
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

	contactSet := contactentity.ContactSubjects(statements)

	facts := contactentity.FactsForContacts(statements, annotations, contactSet)
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
) ([]contactentity.Statement, error) {
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
) ([]contactentity.Statement, error) {
	statements := []contactentity.Statement{}

	for rows.Next() {
		var statement contactentity.Statement

		err := rows.Scan(
			&statement.SourceDigest,
			&statement.StatementHash,
			&statement.Subject,
			&statement.Predicate,
			&statement.Object,
			&statement.ObjectKind,
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
) ([]contactentity.Statement, error) {
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
) ([]contactentity.Annotation, error) {
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
) ([]contactentity.Annotation, error) {
	annotations := []contactentity.Annotation{}

	for rows.Next() {
		var annotation contactentity.Annotation

		err := rows.Scan(
			&annotation.SourceDigest,
			&annotation.StatementHash,
			&annotation.Predicate,
			&annotation.Object,
		)
		if err != nil {
			return nil, fmt.Errorf("scan RDF annotation for contact projection: %w", err)
		}

		annotations = append(annotations, annotation)
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
) ([]contactentity.Annotation, error) {
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

func insertContactFact(
	ctx context.Context,
	transaction pgx.Tx,
	fact contactentity.Fact,
) error {
	metadata, err := json.Marshal(map[string]any{
		"historical": fact.Historical,
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
		fact.ContactID,
		fact.FactKind,
		fact.Value,
		hashString(fact.Value),
		fact.Predicate,
		fact.SourceDigest,
		fact.StatementHash,
		fact.ValidFrom,
		fact.ValidUntil,
		fact.Historical,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("insert contact fact projection: %w", err)
	}

	if fact.FactKind != contactentity.FactKindEmail {
		return nil
	}

	return insertContactIdentityBinding(ctx, transaction, fact)
}

func insertContactIdentityBinding(
	ctx context.Context,
	transaction pgx.Tx,
	fact contactentity.Fact,
) error {
	token := contactentity.NormalizeIdentity(fact.Value)
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
		fact.ContactID,
		fact.StatementHash,
		fact.ValidFrom,
		fact.ValidUntil,
		fact.SourceDigest,
	)
	if err != nil {
		return fmt.Errorf("insert contact identity binding projection: %w", err)
	}

	return nil
}

func refreshContactRollupsTx(ctx context.Context, transaction pgx.Tx) error {
	_, err := transaction.Exec(
		ctx,
		`WITH fact_rollups AS (
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
		string_agg(f.value, ' ' ORDER BY f.fact_kind, f.value) AS search_text
		FROM query_contact_facts f
		GROUP BY f.contact_id
		),
		observation_rollups AS (
		SELECT b.contact_id,
		min(p.message_time) FILTER (WHERE p.message_time IS NOT NULL) AS first_seen_at,
		max(p.message_time) FILTER (WHERE p.message_time IS NOT NULL) AS last_seen_at,
		count(DISTINCT p.message_digest)::integer AS message_count,
		count(*)::integer AS participant_count
		FROM (
		SELECT DISTINCT contact_id, token_hash, token
		FROM query_contact_identity_bindings
		) b
		JOIN query_mail_participants p
		ON p.token_hash = b.token_hash AND p.token = b.token
		GROUP BY b.contact_id
		)
		INSERT INTO query_contact_rollups(
		contact_id, display_name, primary_email, fact_count, search_text,
		first_seen_at, last_seen_at, message_count, participant_count, updated_at
		)
		SELECT f.contact_id,
		f.display_name,
		f.primary_email,
		f.fact_count,
		f.search_text,
		o.first_seen_at,
		o.last_seen_at,
		COALESCE(o.message_count, 0),
		COALESCE(o.participant_count, 0),
		now()
		FROM fact_rollups f
		LEFT JOIN observation_rollups o ON o.contact_id = f.contact_id
		ON CONFLICT(contact_id) DO UPDATE SET
		display_name = excluded.display_name,
		primary_email = excluded.primary_email,
		fact_count = excluded.fact_count,
		search_text = excluded.search_text,
		first_seen_at = excluded.first_seen_at,
		last_seen_at = excluded.last_seen_at,
		message_count = excluded.message_count,
		participant_count = excluded.participant_count,
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
		`WITH fact_rollups AS (
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
		string_agg(f.value, ' ' ORDER BY f.fact_kind, f.value) AS search_text
		FROM query_contact_facts f
		WHERE f.contact_id = ANY($1)
		GROUP BY f.contact_id
		),
		observation_rollups AS (
		SELECT b.contact_id,
		min(p.message_time) FILTER (WHERE p.message_time IS NOT NULL) AS first_seen_at,
		max(p.message_time) FILTER (WHERE p.message_time IS NOT NULL) AS last_seen_at,
		count(DISTINCT p.message_digest)::integer AS message_count,
		count(*)::integer AS participant_count
		FROM (
		SELECT DISTINCT contact_id, token_hash, token
		FROM query_contact_identity_bindings
		WHERE contact_id = ANY($1)
		) b
		JOIN query_mail_participants p
		ON p.token_hash = b.token_hash AND p.token = b.token
		GROUP BY b.contact_id
		)
		INSERT INTO query_contact_rollups(
		contact_id, display_name, primary_email, fact_count, search_text,
		first_seen_at, last_seen_at, message_count, participant_count, updated_at
		)
		SELECT f.contact_id,
		f.display_name,
		f.primary_email,
		f.fact_count,
		f.search_text,
		o.first_seen_at,
		o.last_seen_at,
		COALESCE(o.message_count, 0),
		COALESCE(o.participant_count, 0),
		now()
		FROM fact_rollups f
		LEFT JOIN observation_rollups o ON o.contact_id = f.contact_id
		ON CONFLICT(contact_id) DO UPDATE SET
		display_name = excluded.display_name,
		primary_email = excluded.primary_email,
		fact_count = excluded.fact_count,
		search_text = excluded.search_text,
		first_seen_at = excluded.first_seen_at,
		last_seen_at = excluded.last_seen_at,
		message_count = excluded.message_count,
		participant_count = excluded.participant_count,
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

	var (
		aggregate contracts.ContactAggregate
		firstSeen sql.NullTime
		lastSeen  sql.NullTime
	)

	err := index.pool.QueryRow(
		ctx,
		`SELECT contact_id, display_name, primary_email, fact_count,
		first_seen_at, last_seen_at, message_count, participant_count
		   FROM query_contact_rollups
		  WHERE contact_id = $1`,
		contactID,
	).Scan(
		&aggregate.ContactID,
		&aggregate.DisplayName,
		&aggregate.PrimaryEmail,
		&aggregate.FactCount,
		&firstSeen,
		&lastSeen,
		&aggregate.MessageCount,
		&aggregate.ParticipantCount,
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
	if firstSeen.Valid {
		aggregate.FirstSeenAt = firstSeen.Time
	}

	if lastSeen.Valid {
		aggregate.LastSeenAt = lastSeen.Time
	}

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
		`SELECT contact_id, display_name, primary_email, fact_count,
		first_seen_at, last_seen_at, message_count, participant_count
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
		var (
			result    contracts.ContactSearchResult
			firstSeen sql.NullTime
			lastSeen  sql.NullTime
		)

		err = rows.Scan(
			&result.ContactID,
			&result.DisplayName,
			&result.PrimaryEmail,
			&result.FactCount,
			&firstSeen,
			&lastSeen,
			&result.MessageCount,
			&result.ParticipantCount,
		)
		if err != nil {
			return contracts.ContactSearchResponse{}, fmt.Errorf("scan contact search: %w", err)
		}

		result.Score = 1
		if firstSeen.Valid {
			result.FirstSeenAt = firstSeen.Time
		}

		if lastSeen.Valid {
			result.LastSeenAt = lastSeen.Time
		}

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
	token := contactentity.NormalizeIdentity(request.Identity)
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
