// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"strings"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
	"blackcat.ca/gmeow/internal/observability"
	"blackcat.ca/gmeow/internal/query"
	"blackcat.ca/gmeow/internal/rdfbundle"
)

const rdfTypePredicate = rdfbundle.TypePredicate

// The Turtle/RDF-star parser now lives in the dependency-free internal/rdfbundle
// package (shared with IMPORT). These aliases keep the projection code unchanged
// while the parser is sourced from there.
type (
	rdfTerm       = rdfbundle.Term
	rdfStatement  = rdfbundle.Statement
	rdfAnnotation = rdfbundle.AnnotationRecord
)

type transactionBeginner interface {
	BeginTx(ctx context.Context, txOptions pgx.TxOptions) (pgx.Tx, error)
}

type contactSearchCounter interface {
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
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

	contentBytes, err := io.ReadAll(opened)
	if err != nil {
		return true, fmt.Errorf("read RDF bundle %s: %w", manifest.ObjectDigest, err)
	}

	statements, annotations, err := parseRDFBundle(rdfBundleText(contentBytes))
	if err != nil {
		return true, fmt.Errorf("parse RDF bundle %s: %w", manifest.ObjectDigest, err)
	}

	termIDs := map[rdfTerm]int64{}
	for index, statement := range statements {
		err := insertRDFStatement(
			ctx,
			transaction,
			manifest.ObjectDigest,
			statement,
			index,
			termIDs,
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
			termIDs,
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

func rdfContactSubjectsForSourceTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
) ([]string, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT DISTINCT subj.term_value, obj.term_value
		   FROM query_rdf_statements s
		   JOIN query_rdf_terms subj ON subj.term_id = s.subject_term_id
		   JOIN query_rdf_terms pred ON pred.term_id = s.predicate_term_id
		   JOIN query_rdf_terms obj ON obj.term_id = s.object_term_id
		  WHERE s.source_digest = $1
		    AND pred.term_value = $2`,
		digest,
		rdfTypePredicate,
	)
	if err != nil {
		return nil, fmt.Errorf("query RDF contact subjects: %w", err)
	}
	defer rows.Close()

	subjects := []string{}
	seen := map[string]bool{}

	for rows.Next() {
		var subject string
		var object string

		err = rows.Scan(&subject, &object)
		if err != nil {
			return nil, fmt.Errorf("scan RDF contact subject: %w", err)
		}

		if seen[subject] || !contactentity.IsContactEntityType(object) {
			continue
		}

		seen[subject] = true
		subjects = append(subjects, subject)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate RDF contact subjects: %w", err)
	}

	return subjects, nil
}

func rdfContactRootSubjectsForSourceTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
) ([]string, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT DISTINCT subj.term_value, obj.term_value,
		        EXISTS (
		          SELECT 1
		            FROM query_rdf_statements refs
		           WHERE refs.source_digest = s.source_digest
		             AND refs.object_term_id = s.subject_term_id
		        ) AS referenced_as_object
		   FROM query_rdf_statements s
		   JOIN query_rdf_terms subj ON subj.term_id = s.subject_term_id
		   JOIN query_rdf_terms pred ON pred.term_id = s.predicate_term_id
		   JOIN query_rdf_terms obj ON obj.term_id = s.object_term_id
		  WHERE s.source_digest = $1
		    AND pred.term_value = $2`,
		digest,
		rdfTypePredicate,
	)
	if err != nil {
		return nil, fmt.Errorf("query RDF contact root subjects: %w", err)
	}
	defer rows.Close()

	roots := []string{}
	seen := map[string]bool{}

	for rows.Next() {
		var subject string
		var object string
		var referencedAsObject bool

		err = rows.Scan(&subject, &object, &referencedAsObject)
		if err != nil {
			return nil, fmt.Errorf("scan RDF contact root subject: %w", err)
		}

		if referencedAsObject || seen[subject] || !contactentity.IsContactEntityType(object) {
			continue
		}

		seen[subject] = true
		roots = append(roots, subject)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate RDF contact root subjects: %w", err)
	}

	return roots, nil
}

func contactFactSubjectsForSourceTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
) ([]string, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT DISTINCT contact_id
		   FROM query_contact_facts
		  WHERE source_digest = $1`,
		digest,
	)
	if err != nil {
		return nil, fmt.Errorf("query contact facts for RDF source: %w", err)
	}
	defer rows.Close()

	contacts := []string{}

	for rows.Next() {
		var contact string

		err = rows.Scan(&contact)
		if err != nil {
			return nil, fmt.Errorf("scan contact fact source subject: %w", err)
		}

		contacts = append(contacts, contact)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate contact fact source subjects: %w", err)
	}

	return contacts, nil
}

func rdfContactProjectionSubjectsTx(
	ctx context.Context,
	transaction pgx.Tx,
	manifest contracts.Manifest,
) ([]string, error) {
	explicit := contactProjectionSubjectsFromManifest(manifest)
	if len(explicit) > 0 {
		return explicit, nil
	}

	if rdfSourceBundleNeedsRootInference(manifest) {
		roots, err := rdfContactRootSubjectsForSourceTx(
			ctx,
			transaction,
			manifest.ObjectDigest,
		)
		if err != nil {
			return nil, err
		}
		if len(roots) > 0 {
			return roots, nil
		}
	}

	return rdfContactSubjectsForSourceTx(ctx, transaction, manifest.ObjectDigest)
}

func contactProjectionSubjectsFromManifest(manifest contracts.Manifest) []string {
	subjects := []string{}

	for _, facet := range manifest.Facets {
		switch firstNonEmpty(facet.Kind, facet.Name) {
		case contracts.ContactEntityFacetKind,
			contracts.RDFSourceBundleFacetKind,
			contracts.RDFClaimBundleFacetKind:
		default:
			continue
		}

		metadata := firstMap(facet.Metadata, facet.Attributes)
		subjects = append(subjects, contactentity.RootSubject(metadata))
		subjects = append(subjects, contactentity.TargetSubject(metadata))
	}

	return uniqueNonEmptyStrings(subjects)
}

func rdfSourceBundleNeedsRootInference(manifest contracts.Manifest) bool {
	for _, facet := range manifest.Facets {
		if firstNonEmpty(facet.Kind, facet.Name) != contracts.RDFSourceBundleFacetKind {
			continue
		}

		if rdfClaimKindNeedsRootInference(firstMap(facet.Metadata, facet.Attributes)) {
			return true
		}
	}

	return false
}

func rdfClaimKindNeedsRootInference(metadata map[string]any) bool {
	switch strings.ToLower(strings.TrimSpace(stringMapValue(metadata, "claim_kind"))) {
	case "rdf", "ttl", "turtle", "foaf":
		return true
	default:
		return false
	}
}

func stringMapValue(metadata map[string]any, key string) string {
	value, _ := metadata[key].(string)

	return value
}

func insertRDFStatement(
	ctx context.Context,
	transaction pgx.Tx,
	sourceDigest contracts.ObjectDigest,
	statement rdfStatement,
	order int,
	termIDs map[rdfTerm]int64,
) error {
	subjectID, err := upsertRDFTerm(ctx, transaction, statement.Subject, termIDs)
	if err != nil {
		return err
	}

	predicateID, err := upsertRDFTerm(ctx, transaction, statement.Predicate, termIDs)
	if err != nil {
		return err
	}

	objectID, err := upsertRDFTerm(ctx, transaction, statement.Object, termIDs)
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
		statement.Hash,
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
	termIDs map[rdfTerm]int64,
) error {
	predicateID, err := upsertRDFTerm(ctx, transaction, annotation.Predicate, termIDs)
	if err != nil {
		return err
	}

	objectID, err := upsertRDFTerm(ctx, transaction, annotation.Object, termIDs)
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
		annotation.Hash,
		predicateID,
		objectID,
	)
	if err != nil {
		return fmt.Errorf("insert RDF statement annotation projection: %w", err)
	}

	return nil
}

func rdfBundleText(content []byte) string {
	return strings.ToValidUTF8(string(content), "?")
}

func upsertRDFTerm(
	ctx context.Context,
	transaction pgx.Tx,
	term rdfTerm,
	termIDs map[rdfTerm]int64,
) (int64, error) {
	if termID, found := termIDs[term]; found {
		return termID, nil
	}

	var termID int64

	err := transaction.QueryRow(
		ctx,
		`WITH inserted AS (
		   INSERT INTO query_rdf_terms(term_kind, term_value, language, datatype, term_key)
		   VALUES($1,$2,$3,$4,$5)
		   ON CONFLICT(term_kind, term_key, language, datatype) DO NOTHING
		   RETURNING term_id
		 )
		 SELECT term_id FROM inserted
		 UNION ALL
		 SELECT term_id
		   FROM query_rdf_terms
		  WHERE term_kind = $1
		    AND term_key = $5
		    AND language = $3
		    AND datatype = $4
		 LIMIT 1`,
		term.Kind,
		term.Value,
		term.Language,
		term.Datatype,
		rdfTermKey(term),
	).Scan(&termID)
	if err != nil {
		return 0, fmt.Errorf("upsert RDF term: %w", err)
	}
	termIDs[term] = termID

	return termID, nil
}

// parseRDFBundle and rdfTermKey delegate to the shared internal/rdfbundle parser;
// the projection consumes the same statement/term types via the aliases above.
func parseRDFBundle(content string) ([]rdfStatement, []rdfAnnotation, error) {
	return rdfbundle.Parse(content)
}

func rdfTermKey(term rdfTerm) string {
	return rdfbundle.TermKey(term)
}

func refreshContactProjectionTx(ctx context.Context, transaction pgx.Tx) error {
	for _, table := range []string{
		"active_contact_aliases",
		"query_contact_aliases",
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

	statements, contactSet, err := filterProjectedContactStatementsTx(
		ctx,
		transaction,
		statements,
	)
	if err != nil {
		return err
	}

	facts := contactentity.FactsForContacts(statements, annotations, contactSet)
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
		"active_contact_aliases",
		"query_contact_aliases",
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

	statements, contactSet, err := filterProjectedContactStatementsTx(
		ctx,
		transaction,
		statements,
	)
	if err != nil {
		return err
	}

	facts := contactentity.FactsForContacts(statements, annotations, contactSet)
	for _, fact := range facts {
		err := insertContactFact(ctx, transaction, fact)
		if err != nil {
			return err
		}
	}

	return refreshContactRollupsForContactsTx(ctx, transaction, contacts)
}

func filterProjectedContactStatementsTx(
	ctx context.Context,
	transaction pgx.Tx,
	statements []contactentity.Statement,
) ([]contactentity.Statement, map[string]bool, error) {
	bySource := map[contracts.ObjectDigest][]contactentity.Statement{}
	for _, statement := range statements {
		bySource[statement.SourceDigest] = append(bySource[statement.SourceDigest], statement)
	}

	contacts := map[string]bool{}
	filtered := make([]contactentity.Statement, 0, len(statements))
	for digest, sourceStatements := range bySource {
		sourceContacts, err := rdfProjectedContactsForSourceStatementsTx(
			ctx,
			transaction,
			digest,
			sourceStatements,
		)
		if err != nil {
			return nil, nil, err
		}

		for _, statement := range sourceStatements {
			if !sourceContacts[statement.Subject] {
				continue
			}

			filtered = append(filtered, statement)
			contacts[statement.Subject] = true
		}
	}

	return filtered, contacts, nil
}

func rdfProjectedContactsForSourceStatementsTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
	statements []contactentity.Statement,
) (map[string]bool, error) {
	typedContacts := contactentity.ContactSubjects(statements)

	explicit, inferRoots, err := rdfContactProjectionPolicyForDigestTx(
		ctx,
		transaction,
		digest,
	)
	if err != nil {
		return nil, err
	}

	if len(explicit) > 0 {
		return contactSetFromSubjects(explicit), nil
	}

	if inferRoots {
		roots, err := rdfContactRootSubjectsForSourceTx(ctx, transaction, digest)
		if err != nil {
			return nil, err
		}
		if len(roots) > 0 {
			return typedContactSetFromSubjects(roots, typedContacts), nil
		}
	}

	return typedContacts, nil
}

func rdfContactProjectionPolicyForDigestTx(
	ctx context.Context,
	transaction pgx.Tx,
	digest contracts.ObjectDigest,
) ([]string, bool, error) {
	rows, err := transaction.Query(
		ctx,
		`SELECT kind, metadata_json
		   FROM query_object_facets
		  WHERE object_digest = $1
		    AND kind = ANY($2)`,
		digest,
		[]string{
			contracts.ContactEntityFacetKind,
			contracts.RDFSourceBundleFacetKind,
			contracts.RDFClaimBundleFacetKind,
		},
	)
	if err != nil {
		return nil, false, fmt.Errorf("query RDF contact projection policy: %w", err)
	}
	defer rows.Close()

	explicit := []string{}
	inferRoots := false

	for rows.Next() {
		var kind string
		var encoded []byte

		err = rows.Scan(&kind, &encoded)
		if err != nil {
			return nil, false, fmt.Errorf("scan RDF contact projection policy: %w", err)
		}

		metadata := map[string]any{}
		if len(encoded) > 0 {
			err = json.Unmarshal(encoded, &metadata)
			if err != nil {
				return nil, false, fmt.Errorf("decode RDF contact projection policy: %w", err)
			}
		}

		explicit = append(explicit, contactentity.RootSubject(metadata))
		explicit = append(explicit, contactentity.TargetSubject(metadata))

		if kind == contracts.RDFSourceBundleFacetKind &&
			rdfClaimKindNeedsRootInference(metadata) {
			inferRoots = true
		}
	}

	err = rows.Err()
	if err != nil {
		return nil, false, fmt.Errorf("iterate RDF contact projection policy: %w", err)
	}

	return uniqueNonEmptyStrings(explicit), inferRoots, nil
}

func contactSetFromSubjects(subjects []string) map[string]bool {
	contacts := map[string]bool{}
	for _, subject := range subjects {
		contacts[subject] = true
	}

	return contacts
}

func typedContactSetFromSubjects(subjects []string, typedContacts map[string]bool) map[string]bool {
	contacts := map[string]bool{}
	for _, subject := range subjects {
		if typedContacts[subject] {
			contacts[subject] = true
		}
	}

	return contacts
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
		if fact.FactKind == contactentity.FactKindContactAlias {
			return insertContactAlias(ctx, transaction, fact)
		}

		return nil
	}

	return insertContactIdentityBinding(ctx, transaction, fact)
}

func insertContactAlias(
	ctx context.Context,
	transaction pgx.Tx,
	fact contactentity.Fact,
) error {
	alias := contactentity.NormalizeAlias(fact.Value)
	if alias == "" {
		return nil
	}

	if fact.ValidUntil == "" {
		claimed, existingContactID, err := claimActiveContactAlias(
			ctx,
			transaction,
			alias,
			fact.ContactID,
		)
		if err != nil {
			return err
		}
		if !claimed {
			observability.Logger(ctx).Warn(
				"skipping conflicting contact alias",
				"alias",
				alias,
				"contact_id",
				fact.ContactID,
				"existing_contact_id",
				existingContactID,
			)

			return nil
		}
	}

	_, err := transaction.Exec(
		ctx,
		`INSERT INTO query_contact_aliases(
		   contact_alias, contact_id, statement_hash, source_digest,
		   valid_from, valid_until, projected_at
		 ) VALUES($1,$2,$3,$4,$5,$6,now())
		 ON CONFLICT(contact_alias, contact_id, statement_hash) DO UPDATE SET
		   source_digest = excluded.source_digest,
		   valid_from = COALESCE(
		     NULLIF(excluded.valid_from, ''),
		     query_contact_aliases.valid_from
		   ),
		   valid_until = COALESCE(
		     NULLIF(excluded.valid_until, ''),
		     query_contact_aliases.valid_until
		   ),
		   projected_at = now()`,
		alias,
		fact.ContactID,
		fact.StatementHash,
		fact.SourceDigest,
		fact.ValidFrom,
		fact.ValidUntil,
	)
	if err != nil {
		return fmt.Errorf("insert contact alias projection: %w", err)
	}

	return nil
}

func claimActiveContactAlias(
	ctx context.Context,
	transaction pgx.Tx,
	alias string,
	contactID string,
) (bool, string, error) {
	var claimedContactID string
	err := transaction.QueryRow(
		ctx,
		`INSERT INTO active_contact_aliases(
		   contact_alias, contact_id, projected_at
		 ) VALUES($1,$2,now())
		 ON CONFLICT(contact_alias) DO UPDATE SET
		   contact_id = excluded.contact_id,
		   projected_at = now()
		 WHERE active_contact_aliases.contact_id = excluded.contact_id
		 RETURNING contact_id`,
		alias,
		contactID,
	).Scan(&claimedContactID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			existingContactID, lookupErr := activeContactAliasOwner(
				ctx,
				transaction,
				alias,
			)
			if lookupErr != nil {
				return false, "", lookupErr
			}

			return false, existingContactID, nil
		}

		return false, "", fmt.Errorf("claim active contact alias: %w", err)
	}

	return true, claimedContactID, nil
}

func activeContactAliasOwner(
	ctx context.Context,
	transaction pgx.Tx,
	alias string,
) (string, error) {
	var contactID string
	err := transaction.QueryRow(
		ctx,
		`SELECT contact_id
		   FROM active_contact_aliases
		  WHERE contact_alias = $1`,
		alias,
	).Scan(&contactID)
	if err != nil {
		return "", fmt.Errorf("lookup active contact alias owner: %w", err)
	}

	return contactID, nil
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
		COALESCE(
		max(NULLIF(f.value, '')::integer) FILTER (
		WHERE f.fact_kind = 'importance' AND f.value ~ '^[0-9]+$'
		),
		0
		)::integer AS importance_level,
		string_agg(f.value, ' ' ORDER BY f.fact_kind, f.value) AS search_text
		FROM query_contact_facts f
		GROUP BY f.contact_id
		),
		observation_rollups AS (
		SELECT b.contact_id,
		min(p.message_time) AS first_seen_at,
		max(p.message_time) AS last_seen_at,
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
		importance_level, first_seen_at, last_seen_at, message_count,
		participant_count, updated_at
		)
		SELECT f.contact_id,
		f.display_name,
		f.primary_email,
		f.fact_count,
		f.search_text,
		f.importance_level,
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
		importance_level = GREATEST(
		  query_contact_rollups.importance_level,
		  excluded.importance_level
		),
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
		COALESCE(
		max(NULLIF(f.value, '')::integer) FILTER (
		WHERE f.fact_kind = 'importance' AND f.value ~ '^[0-9]+$'
		),
		0
		)::integer AS importance_level,
		string_agg(f.value, ' ' ORDER BY f.fact_kind, f.value) AS search_text
		FROM query_contact_facts f
		WHERE f.contact_id = ANY($1)
		GROUP BY f.contact_id
		),
		observation_rollups AS (
		SELECT b.contact_id,
		min(p.message_time) AS first_seen_at,
		max(p.message_time) AS last_seen_at,
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
		importance_level, first_seen_at, last_seen_at, message_count,
		participant_count, updated_at
		)
		SELECT f.contact_id,
		f.display_name,
		f.primary_email,
		f.fact_count,
		f.search_text,
		f.importance_level,
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
		importance_level = GREATEST(
		  query_contact_rollups.importance_level,
		  excluded.importance_level
		),
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
	contactID, err := index.resolveContactRef(ctx, request.ContactID)
	if err != nil {
		return contracts.ContactAggregate{}, err
	}

	var (
		aggregate contracts.ContactAggregate
		firstSeen sql.NullTime
		lastSeen  sql.NullTime
	)

	err = index.pool.QueryRow(
		ctx,
		`SELECT contact_id, display_name, primary_email, fact_count, importance_level,
		first_seen_at, last_seen_at, message_count, participant_count
		   FROM query_contact_rollups
		  WHERE contact_id = $1`,
		contactID,
	).Scan(
		&aggregate.ContactID,
		&aggregate.DisplayName,
		&aggregate.PrimaryEmail,
		&aggregate.FactCount,
		&aggregate.ImportanceLevel,
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
	aliases, err := index.aliasesForContacts(ctx, []string{aggregate.ContactID})
	if err != nil {
		return contracts.ContactAggregate{}, err
	}
	aggregate.Aliases = aliases[aggregate.ContactID]

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
		`SELECT contact_id, display_name, primary_email, fact_count, importance_level,
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
	contactIDs := []string{}

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
			&result.ImportanceLevel,
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

		contactIDs = append(contactIDs, result.ContactID)
		results = append(results, result)
	}

	err = rows.Err()
	if err != nil {
		return contracts.ContactSearchResponse{}, fmt.Errorf(
			"iterate contact search: %w",
			err,
		)
	}

	aliases, err := index.aliasesForContacts(ctx, contactIDs)
	if err != nil {
		return contracts.ContactSearchResponse{}, err
	}
	for index := range results {
		results[index].Aliases = aliases[results[index].ContactID]
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
	contactID, err := index.resolveContactRef(ctx, request.Identity)
	if err != nil {
		return contracts.ContactIdentityResolveResponse{}, err
	}
	if contactID != "" && contactID != strings.TrimSpace(request.Identity) {
		return contracts.ContactIdentityResolveResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
			ContactIDs:    []string{contactID},
		}, nil
	}

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
