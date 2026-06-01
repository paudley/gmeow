// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"unicode/utf8"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
)

const (
	contactAnalysisInputLimitBytes = 12000
	contactAnalysisFactLimit       = 200
	contactFactArgCapacity         = 4
	contactIdentityArgCapacity     = 2
)

func (index *Index) ContactFacts(
	ctx context.Context,
	request contracts.ContactFactRequest,
) (contracts.ContactFactResponse, error) {
	contactIDs, err := index.resolveContactRefs(ctx, request.ContactIDs)
	if err != nil {
		return contracts.ContactFactResponse{}, err
	}
	request.ContactIDs = contactIDs

	args := make([]any, 0, contactFactArgCapacity)
	where := contactFactWhere(&args, request)
	limit := normalizedLimit(request.Limit)
	offset := max(request.Offset, 0)

	total, err := countContactIntelligence(
		ctx,
		index.pool,
		"query_contact_facts f",
		where,
		args,
	)
	if err != nil {
		return contracts.ContactFactResponse{}, err
	}

	args = append(args, limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT f.source_digest, f.statement_hash, f.contact_id, f.fact_kind,
		        f.value, f.predicate, f.valid_from, f.valid_until, f.historical
		   FROM query_contact_facts f
		  WHERE %s
		  ORDER BY f.contact_id, f.fact_kind, f.value, f.statement_hash
		  LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.ContactFactResponse{}, fmt.Errorf("query contact facts: %w", err)
	}
	defer rows.Close()

	facts := []contracts.ContactFact{}

	for rows.Next() {
		var fact contracts.ContactFact

		err = rows.Scan(
			&fact.SourceDigest,
			&fact.StatementHash,
			&fact.ContactID,
			&fact.FactKind,
			&fact.Value,
			&fact.Predicate,
			&fact.ValidFrom,
			&fact.ValidUntil,
			&fact.Historical,
		)
		if err != nil {
			return contracts.ContactFactResponse{}, fmt.Errorf("scan contact fact: %w", err)
		}

		facts = append(facts, fact)
	}

	err = rows.Err()
	if err != nil {
		return contracts.ContactFactResponse{}, fmt.Errorf("iterate contact facts: %w", err)
	}

	return contracts.ContactFactResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Facts:         facts,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (index *Index) ContactIdentityDetails(
	ctx context.Context,
	request contracts.ContactIdentityDetailRequest,
) (contracts.ContactIdentityDetailResponse, error) {
	contactIDs, err := index.resolveContactRefs(ctx, request.ContactIDs)
	if err != nil {
		return contracts.ContactIdentityDetailResponse{}, err
	}
	request.ContactIDs = contactIDs

	matchedTokens := normalizedIdentityTokens(request.Identities)
	args, where := contactIdentityDetailWhere(matchedTokens, request.ContactIDs)

	limit := normalizedLimit(request.Limit)
	offset := max(request.Offset, 0)

	total, err := countContactIntelligence(
		ctx,
		index.pool,
		"query_contact_identity_bindings b",
		where,
		args,
	)
	if err != nil {
		return contracts.ContactIdentityDetailResponse{}, err
	}

	args = append(args, limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT b.token_hash, b.token, b.contact_id, b.statement_hash,
		        b.valid_from, b.valid_until, b.source_digest
		   FROM query_contact_identity_bindings b
		  WHERE %s
		  ORDER BY b.contact_id, b.token, b.statement_hash
		  LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.ContactIdentityDetailResponse{}, fmt.Errorf(
			"query contact identity details: %w",
			err,
		)
	}
	defer rows.Close()

	results, err := scanContactIdentityDetails(rows, matchedTokens)
	if err != nil {
		return contracts.ContactIdentityDetailResponse{}, err
	}

	return contracts.ContactIdentityDetailResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (index *Index) ContactNeighborhood(
	ctx context.Context,
	request contracts.ContactNeighborhoodRequest,
) (contracts.ContactNeighborhoodResponse, error) {
	contactID, err := index.resolveContactRef(ctx, request.ContactID)
	if err != nil {
		return contracts.ContactNeighborhoodResponse{}, err
	}
	if contactID == "" {
		return contracts.ContactNeighborhoodResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
			Results:       []contracts.ContactNeighborhoodResult{},
			Limit:         normalizedLimit(request.Limit),
			Offset:        max(request.Offset, 0),
		}, nil
	}

	args, where := contactNeighborhoodWhere(contactID, request.FactKinds)
	limit := normalizedLimit(request.Limit)
	offset := max(request.Offset, 0)

	total, err := countContactIntelligence(
		ctx,
		index.pool,
		"query_contact_facts f",
		where,
		args,
	)
	if err != nil {
		return contracts.ContactNeighborhoodResponse{}, err
	}

	args = append(args, limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT f.source_digest, f.statement_hash, f.contact_id, f.fact_kind,
		        f.value, f.predicate, f.valid_from, f.valid_until
		   FROM query_contact_facts f
		  WHERE %s
		  ORDER BY f.fact_kind, f.value, f.statement_hash
		  LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.ContactNeighborhoodResponse{}, fmt.Errorf(
			"query contact neighborhood: %w",
			err,
		)
	}
	defer rows.Close()

	results, err := scanContactNeighborhood(rows)
	if err != nil {
		return contracts.ContactNeighborhoodResponse{}, err
	}

	return contracts.ContactNeighborhoodResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (index *Index) ContactAnalysisInputs(
	ctx context.Context,
	request contracts.ContactAnalysisInputRequest,
) (contracts.ContactAnalysisInputResponse, error) {
	contactIDs, err := index.resolveContactRefs(ctx, request.ContactIDs)
	if err != nil {
		return contracts.ContactAnalysisInputResponse{}, err
	}
	request.ContactIDs = contactIDs

	args := []any{}
	where := []string{"true"}

	if contacts := uniqueNonEmptyStrings(request.ContactIDs); len(contacts) > 0 {
		args = append(args, contacts)
		where = append(where, fmt.Sprintf("r.contact_id = ANY($%d)", len(args)))
	}

	limit := normalizedLimit(request.Limit)
	offset := max(request.Offset, 0)

	total, err := countContactIntelligence(
		ctx,
		index.pool,
		"query_contact_rollups r",
		where,
		args,
	)
	if err != nil {
		return contracts.ContactAnalysisInputResponse{}, err
	}

	args = append(args, limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT r.contact_id, r.display_name, r.primary_email, r.fact_count,
		        r.first_seen_at, r.last_seen_at, r.message_count, r.participant_count
		   FROM query_contact_rollups r
		  WHERE %s
		  ORDER BY r.display_name, r.contact_id
		  LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.ContactAnalysisInputResponse{}, fmt.Errorf(
			"query contact analysis inputs: %w",
			err,
		)
	}

	results, err := index.contactAnalysisInputResults(ctx, rows, request.FactKinds)
	if err != nil {
		return contracts.ContactAnalysisInputResponse{}, err
	}

	return contracts.ContactAnalysisInputResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func (index *Index) contactAnalysisInputResults(
	ctx context.Context,
	rows pgx.Rows,
	factKinds []string,
) ([]contracts.ContactAnalysisInputResult, error) {
	results := []contracts.ContactAnalysisInputResult{}
	contactIDs := []string{}

	for rows.Next() {
		result, err := scanContactAnalysisInputRollup(rows)
		if err != nil {
			rows.Close()

			return nil, err
		}

		contactIDs = append(contactIDs, result.ContactID)
		results = append(results, result)
	}

	err := rows.Err()
	rows.Close()

	if err != nil {
		return nil, fmt.Errorf("iterate contact analysis inputs: %w", err)
	}

	factsByContact, err := index.contactAnalysisFactsBatch(ctx, contactIDs, factKinds)
	if err != nil {
		return nil, err
	}

	for offset := range results {
		result := &results[offset]
		result.InputText = boundedContactAnalysisText(
			*result,
			factsByContact[result.ContactID],
		)
	}

	return results, nil
}

func contactFactWhere(args *[]any, request contracts.ContactFactRequest) []string {
	where := []string{"true"}

	if contacts := uniqueNonEmptyStrings(request.ContactIDs); len(contacts) > 0 {
		*args = append(*args, contacts)
		where = append(where, fmt.Sprintf("f.contact_id = ANY($%d)", len(*args)))
	}

	if kinds := uniqueNonEmptyStrings(request.FactKinds); len(kinds) > 0 {
		*args = append(*args, kinds)
		where = append(where, fmt.Sprintf("f.fact_kind = ANY($%d)", len(*args)))
	}

	if request.Current {
		where = append(where, "f.historical = false")
		where = append(where, "f.valid_until = ''")
	}

	if at := strings.TrimSpace(request.At); at != "" {
		*args = append(*args, at)
		where = append(where, fmt.Sprintf(
			"(f.valid_from = '' OR f.valid_from <= $%d)",
			len(*args),
		))
		where = append(where, fmt.Sprintf(
			"(f.valid_until = '' OR f.valid_until >= $%d)",
			len(*args),
		))
	}

	if from := strings.TrimSpace(request.From); from != "" {
		*args = append(*args, from)
		where = append(where, fmt.Sprintf(
			"(f.valid_until = '' OR f.valid_until >= $%d)",
			len(*args),
		))
	}

	if until := strings.TrimSpace(request.Until); until != "" {
		*args = append(*args, until)
		where = append(where, fmt.Sprintf(
			"(f.valid_from = '' OR f.valid_from <= $%d)",
			len(*args),
		))
	}

	return where
}

func countContactIntelligence(
	ctx context.Context,
	counter contactSearchCounter,
	tableSQL string,
	where []string,
	args []any,
) (int, error) {
	total := 0

	err := counter.QueryRow(
		ctx,
		"SELECT count(*)::integer FROM "+tableSQL+" WHERE "+strings.Join(where, " AND "),
		args...,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count contact intelligence rows: %w", err)
	}

	return total, nil
}

func normalizedIdentityTokens(values []string) []string {
	tokens := []string{}

	for _, value := range values {
		token := contactentity.NormalizeIdentity(value)
		if token != "" {
			tokens = append(tokens, token)
		}
	}

	return uniqueNonEmptyStrings(tokens)
}

func contactIdentityDetailWhere(
	matchedTokens []string,
	contactIDs []string,
) ([]any, []string) {
	args := make([]any, 0, contactIdentityArgCapacity)
	where := []string{"true"}

	if len(matchedTokens) > 0 {
		hashes := make([]string, 0, len(matchedTokens))
		for _, token := range matchedTokens {
			hashes = append(hashes, hashString(token))
		}

		args = append(args, hashes)
		where = append(where, fmt.Sprintf("b.token_hash = ANY($%d)", len(args)))
	}

	if contacts := uniqueNonEmptyStrings(contactIDs); len(contacts) > 0 {
		args = append(args, contacts)
		where = append(where, fmt.Sprintf("b.contact_id = ANY($%d)", len(args)))
	}

	return args, where
}

func scanContactIdentityDetails(
	rows pgx.Rows,
	matchedTokens []string,
) ([]contracts.ContactIdentityDetail, error) {
	results := []contracts.ContactIdentityDetail{}

	for rows.Next() {
		var result contracts.ContactIdentityDetail

		err := rows.Scan(
			&result.TokenHash,
			&result.Token,
			&result.ContactID,
			&result.StatementHash,
			&result.ValidFrom,
			&result.ValidUntil,
			&result.SourceDigest,
		)
		if err != nil {
			return nil, fmt.Errorf("scan contact identity detail: %w", err)
		}

		result.MatchedToken = matchingIdentityToken(result.Token, matchedTokens)
		results = append(results, result)
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate contact identity details: %w", err)
	}

	return results, nil
}

func matchingIdentityToken(token string, filters []string) string {
	normalizedToken := contactentity.NormalizeIdentity(token)

	if len(filters) == 0 {
		return firstNonEmpty(normalizedToken, token)
	}

	for _, filter := range filters {
		if filter == normalizedToken {
			return filter
		}
	}

	return ""
}

func contactNeighborhoodWhere(contactID string, factKinds []string) ([]any, []string) {
	args := []any{contactID}
	where := []string{"f.contact_id = $1"}

	if kinds := uniqueNonEmptyStrings(factKinds); len(kinds) > 0 {
		args = append(args, kinds)
		where = append(where, fmt.Sprintf("f.fact_kind = ANY($%d)", len(args)))
	} else {
		args = append(args, contactNeighborhoodFactKinds())
		where = append(where, fmt.Sprintf("f.fact_kind = ANY($%d)", len(args)))
	}

	return args, where
}

func contactNeighborhoodFactKinds() []string {
	return []string{
		contactentity.FactKindAffiliation,
		contactentity.FactKindAlias,
		contactentity.FactKindIdentifier,
		contactentity.FactKindRelationship,
		contactentity.FactKindTitle,
		contactentity.FactKindURL,
	}
}

func scanContactNeighborhood(
	rows pgx.Rows,
) ([]contracts.ContactNeighborhoodResult, error) {
	results := []contracts.ContactNeighborhoodResult{}

	for rows.Next() {
		var result contracts.ContactNeighborhoodResult

		err := rows.Scan(
			&result.SourceDigest,
			&result.StatementHash,
			&result.ContactID,
			&result.FactKind,
			&result.Value,
			&result.Predicate,
			&result.ValidFrom,
			&result.ValidUntil,
		)
		if err != nil {
			return nil, fmt.Errorf("scan contact neighborhood: %w", err)
		}

		results = append(results, result)
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate contact neighborhood: %w", err)
	}

	return results, nil
}

func scanContactAnalysisInputRollup(
	rows pgx.Rows,
) (contracts.ContactAnalysisInputResult, error) {
	var (
		result    contracts.ContactAnalysisInputResult
		firstSeen sql.NullTime
		lastSeen  sql.NullTime
	)

	err := rows.Scan(
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
		return contracts.ContactAnalysisInputResult{}, fmt.Errorf(
			"scan contact analysis input: %w",
			err,
		)
	}

	if firstSeen.Valid {
		result.FirstSeenAt = firstSeen.Time
	}

	if lastSeen.Valid {
		result.LastSeenAt = lastSeen.Time
	}

	return result, nil
}

func (index *Index) contactAnalysisFactsBatch(
	ctx context.Context,
	contactIDs []string,
	factKinds []string,
) (map[string][]contracts.ContactFact, error) {
	contacts := uniqueNonEmptyStrings(contactIDs)
	if len(contacts) == 0 {
		return map[string][]contracts.ContactFact{}, nil
	}

	args := []any{contacts}
	where := []string{"f.contact_id = ANY($1)"}

	if kinds := uniqueNonEmptyStrings(factKinds); len(kinds) > 0 {
		args = append(args, kinds)
		where = append(where, fmt.Sprintf("f.fact_kind = ANY($%d)", len(args)))
	}

	args = append(args, contactAnalysisFactLimit)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`SELECT source_digest, statement_hash, contact_id, fact_kind,
value, predicate, valid_from, valid_until, historical
FROM (
SELECT f.source_digest, f.statement_hash, f.contact_id, f.fact_kind,
f.value, f.predicate, f.valid_from, f.valid_until, f.historical,
row_number() OVER (
PARTITION BY f.contact_id
ORDER BY f.fact_kind, f.value, f.statement_hash
) AS fact_rank
FROM query_contact_facts f
WHERE %s
) ranked_facts
WHERE fact_rank <= $%d
ORDER BY contact_id, fact_kind, value, statement_hash`,
		strings.Join(where, " AND "),
		len(args),
	), args...)
	if err != nil {
		return nil, fmt.Errorf("query contact analysis fact batch: %w", err)
	}
	defer rows.Close()

	factsByContact := map[string][]contracts.ContactFact{}

	for rows.Next() {
		var fact contracts.ContactFact

		err = rows.Scan(
			&fact.SourceDigest,
			&fact.StatementHash,
			&fact.ContactID,
			&fact.FactKind,
			&fact.Value,
			&fact.Predicate,
			&fact.ValidFrom,
			&fact.ValidUntil,
			&fact.Historical,
		)
		if err != nil {
			return nil, fmt.Errorf("scan contact analysis fact batch: %w", err)
		}

		factsByContact[fact.ContactID] = append(factsByContact[fact.ContactID], fact)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate contact analysis fact batch: %w", err)
	}

	return factsByContact, nil
}

func boundedContactAnalysisText(
	result contracts.ContactAnalysisInputResult,
	facts []contracts.ContactFact,
) string {
	parts := []string{
		"Contact: " + firstNonEmpty(result.DisplayName, result.ContactID),
	}
	if result.PrimaryEmail != "" {
		parts = append(parts, "Primary email: "+result.PrimaryEmail)
	}

	if !result.FirstSeenAt.IsZero() {
		parts = append(
			parts,
			"First seen: "+result.FirstSeenAt.Format("2006-01-02T15:04:05Z07:00"),
		)
	}

	if !result.LastSeenAt.IsZero() {
		parts = append(
			parts,
			"Last seen: "+result.LastSeenAt.Format("2006-01-02T15:04:05Z07:00"),
		)
	}

	parts = append(parts, fmt.Sprintf(
		"Counts: %d facts, %d messages, %d participants",
		result.FactCount,
		result.MessageCount,
		result.ParticipantCount,
	))

	for _, fact := range facts {
		line := fact.FactKind + ": " + fact.Value
		if fact.ValidFrom != "" || fact.ValidUntil != "" {
			line += " (" + firstNonEmpty(fact.ValidFrom, "..") + " to " +
				firstNonEmpty(fact.ValidUntil, "..") + ")"
		}

		if fact.Historical {
			line += " [historical]"
		}

		parts = append(parts, line)
	}

	text := strings.Join(parts, "\n")
	if len(text) <= contactAnalysisInputLimitBytes {
		return text
	}

	limit := contactAnalysisInputLimitBytes
	for limit > 0 && !utf8.RuneStart(text[limit]) {
		limit--
	}

	return text[:limit]
}

func (index *Index) StoreContactEmbedding(
	ctx context.Context,
	record contracts.ContactEmbeddingUpsert,
) error {
	contactID, err := index.resolveContactRef(ctx, record.ContactID)
	if err != nil {
		return err
	}
	if contactID == "" {
		return fmt.Errorf("contact_id is required")
	}
	if strings.TrimSpace(record.AnalyzerName) == "" {
		return fmt.Errorf("analyzer_name is required")
	}
	if strings.TrimSpace(record.AnalyzerVersion) == "" {
		return fmt.Errorf("analyzer_version is required")
	}
	if strings.TrimSpace(record.InputHash) == "" {
		return fmt.Errorf("input_hash is required")
	}

	status := firstNonEmpty(strings.TrimSpace(record.Status), "complete")
	metadata, err := json.Marshal(record.Metadata)
	if err != nil {
		return fmt.Errorf("encode contact embedding metadata: %w", err)
	}
	if len(metadata) == 0 || string(metadata) == "null" {
		metadata = []byte(`{}`)
	}

	tx, err := index.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin contact embedding upsert: %w", err)
	}
	defer tx.Rollback(ctx)

	_, err = tx.Exec(
		ctx,
		`INSERT INTO query_contact_analysis(
		   contact_id, analyzer_name, analyzer_version, status, model,
		   input_hash, input_bytes, generated_at, data_json
		 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9)
		 ON CONFLICT(contact_id, analyzer_name, analyzer_version, model, input_hash)
		 DO UPDATE SET
		   status = excluded.status,
		   input_bytes = excluded.input_bytes,
		   generated_at = excluded.generated_at,
		   data_json = excluded.data_json`,
		contactID,
		record.AnalyzerName,
		record.AnalyzerVersion,
		status,
		record.Model,
		record.InputHash,
		record.InputBytes,
		record.GeneratedAt,
		metadata,
	)
	if err != nil {
		return fmt.Errorf("upsert contact analysis: %w", err)
	}

	if len(record.Vector) > 0 && status == "complete" {
		embeddingID := contactEmbeddingID(record)
		_, err = tx.Exec(
			ctx,
			`INSERT INTO query_contact_embeddings(
			   contact_id, model, embedding_id, input_hash, text_preview,
			   metadata_json, dimensions, embedding
			 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8::vector)
			 ON CONFLICT(contact_id, model) DO UPDATE SET
			   embedding_id = excluded.embedding_id,
			   input_hash = excluded.input_hash,
			   text_preview = excluded.text_preview,
			   metadata_json = excluded.metadata_json,
			   dimensions = excluded.dimensions,
			   embedding = excluded.embedding`,
			contactID,
			record.Model,
			embeddingID,
			record.InputHash,
			record.TextPreview,
			metadata,
			len(record.Vector),
			vectorLiteral(record.Vector),
		)
		if err != nil {
			return fmt.Errorf("upsert contact embedding: %w", err)
		}
	} else {
		_, err = tx.Exec(
			ctx,
			`DELETE FROM query_contact_embeddings
			  WHERE contact_id = $1 AND model = $2`,
			contactID,
			record.Model,
		)
		if err != nil {
			return fmt.Errorf("delete contact embeddings: %w", err)
		}
	}

	if err = tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit contact embedding upsert: %w", err)
	}

	return nil
}

func contactEmbeddingID(record contracts.ContactEmbeddingUpsert) string {
	return record.AnalyzerName + ":" + record.AnalyzerVersion + ":" + record.InputHash
}

func (index *Index) ContactAnalysisStatus(
	ctx context.Context,
	request contracts.ContactAnalysisStatusRequest,
) (contracts.ContactAnalysisStatusResponse, error) {
	contactIDs, err := index.resolveContactRefs(ctx, request.ContactIDs)
	if err != nil {
		return contracts.ContactAnalysisStatusResponse{}, err
	}
	request.ContactIDs = contactIDs

	args, where := contactAnalysisStatusWhere(request)
	total, err := countContactIntelligence(
		ctx,
		index.pool,
		"query_contact_analysis a",
		where,
		args,
	)
	if err != nil {
		return contracts.ContactAnalysisStatusResponse{}, err
	}

	limit := normalizedLimit(request.Limit)
	offset := max(request.Offset, 0)
	limitPlaceholder := len(args) + 1
	offsetPlaceholder := len(args) + 2
	args = append(args, limit, offset)
	rows, err := index.pool.Query(
		ctx,
		`SELECT contact_id, analyzer_name, analyzer_version, status, model, input_hash, input_bytes, generated_at, data_json
		   FROM query_contact_analysis a
		  WHERE `+strings.Join(
			where,
			" AND ",
		)+`
		  ORDER BY generated_at DESC NULLS LAST, contact_id, analyzer_name
		  LIMIT $`+strconv.Itoa(
			limitPlaceholder,
		)+` OFFSET $`+strconv.Itoa(
			offsetPlaceholder,
		),
		args...,
	)
	if err != nil {
		return contracts.ContactAnalysisStatusResponse{}, fmt.Errorf(
			"query contact analysis status: %w",
			err,
		)
	}
	defer rows.Close()

	results, err := scanContactAnalysisStatus(rows)
	if err != nil {
		return contracts.ContactAnalysisStatusResponse{}, err
	}

	return contracts.ContactAnalysisStatusResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func contactAnalysisStatusWhere(
	request contracts.ContactAnalysisStatusRequest,
) ([]any, []string) {
	args := []any{}
	where := []string{"true"}

	if contacts := uniqueNonEmptyStrings(request.ContactIDs); len(contacts) > 0 {
		args = append(args, contacts)
		where = append(where, fmt.Sprintf("a.contact_id = ANY($%d)", len(args)))
	}
	if hashes := uniqueNonEmptyStrings(request.InputHashes); len(hashes) > 0 {
		args = append(args, hashes)
		where = append(where, fmt.Sprintf("a.input_hash = ANY($%d)", len(args)))
	}
	if analyzerName := strings.TrimSpace(request.AnalyzerName); analyzerName != "" {
		args = append(args, analyzerName)
		where = append(where, fmt.Sprintf("a.analyzer_name = $%d", len(args)))
	}
	if analyzerVersion := strings.TrimSpace(
		request.AnalyzerVersion,
	); analyzerVersion != "" {
		args = append(args, analyzerVersion)
		where = append(where, fmt.Sprintf("a.analyzer_version = $%d", len(args)))
	}
	if model := strings.TrimSpace(request.Model); model != "" {
		args = append(args, model)
		where = append(where, fmt.Sprintf("a.model = $%d", len(args)))
	}

	return args, where
}

func scanContactAnalysisStatus(
	rows pgx.Rows,
) ([]contracts.ContactAnalysisStatusResult, error) {
	results := []contracts.ContactAnalysisStatusResult{}
	for rows.Next() {
		var (
			result      contracts.ContactAnalysisStatusResult
			generatedAt sql.NullTime
			metadata    []byte
		)
		if err := rows.Scan(
			&result.ContactID,
			&result.AnalyzerName,
			&result.AnalyzerVersion,
			&result.Status,
			&result.Model,
			&result.InputHash,
			&result.InputBytes,
			&generatedAt,
			&metadata,
		); err != nil {
			return nil, fmt.Errorf("scan contact analysis status: %w", err)
		}
		if generatedAt.Valid {
			result.GeneratedAt = generatedAt.Time
		}
		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &result.Metadata); err != nil {
				return nil, fmt.Errorf("decode contact analysis status metadata: %w", err)
			}
		}
		if result.Metadata == nil {
			result.Metadata = map[string]any{}
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contact analysis status: %w", err)
	}

	return results, nil
}

func (index *Index) ContactVectorSearch(
	ctx context.Context,
	request contracts.ContactVectorSearchRequest,
) (contracts.ContactVectorSearchResponse, error) {
	if len(request.Vector) == 0 {
		return contracts.ContactVectorSearchResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
		}, nil
	}
	if request.Dimensions > 0 && request.Dimensions != len(request.Vector) {
		return contracts.ContactVectorSearchResponse{}, fmt.Errorf(
			"dimensions (%d) must match vector length (%d)",
			request.Dimensions,
			len(request.Vector),
		)
	}

	contactIDs, err := index.resolveContactRefs(ctx, request.ContactIDs)
	if err != nil {
		return contracts.ContactVectorSearchResponse{}, err
	}
	request.ContactIDs = contactIDs

	args := []any{vectorLiteral(request.Vector), len(request.Vector)}
	where := []string{
		"e.embedding IS NOT NULL",
		"e.dimensions = $2",
	}
	if request.Model != "" {
		args = append(args, request.Model)
		where = append(where, fmt.Sprintf("e.model = $%d", len(args)))
	}
	if request.Dimensions > 0 {
		args = append(args, request.Dimensions)
		where = append(where, fmt.Sprintf("e.dimensions = $%d", len(args)))
	}
	if contacts := uniqueNonEmptyStrings(request.ContactIDs); len(contacts) > 0 {
		args = append(args, contacts)
		where = append(where, fmt.Sprintf("e.contact_id = ANY($%d)", len(args)))
	}

	args = append(args, normalizedLimit(request.Limit))
	sqlText := fmt.Sprintf(
		"SELECT best.contact_id, COALESCE(r.display_name, ''), COALESCE(r.primary_email, ''),\n"+
			"       best.model, best.embedding_id, best.input_hash, best.text_preview,\n"+
			"       best.dimensions, best.distance\n"+
			"  FROM (\n"+
			"        SELECT DISTINCT ON (ranked.contact_id)\n"+
			"               ranked.contact_id, ranked.model, ranked.embedding_id,\n"+
			"               ranked.input_hash, ranked.text_preview, ranked.dimensions,\n"+
			"               ranked.distance\n"+
			"          FROM (\n"+
			"                SELECT e.contact_id, e.model, e.embedding_id, e.input_hash,\n"+
			"                       e.text_preview, e.dimensions,\n"+
			"                       e.embedding <=> $1::vector AS distance\n"+
			"                  FROM query_contact_embeddings e\n"+
			"                 WHERE %s\n"+
			"               ) ranked\n"+
			"         ORDER BY ranked.contact_id, ranked.distance\n"+
			"       ) best\n"+
			"  LEFT JOIN query_contact_rollups r ON r.contact_id = best.contact_id\n"+
			" ORDER BY best.distance, best.contact_id\n"+
			" LIMIT $%d",
		strings.Join(where, " AND "),
		len(args),
	)
	rows, err := index.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return contracts.ContactVectorSearchResponse{}, fmt.Errorf(
			"contact vector search: %w",
			err,
		)
	}
	defer rows.Close()

	results, err := scanContactVectorSearchRows(rows)
	if err != nil {
		return contracts.ContactVectorSearchResponse{}, err
	}

	return contracts.ContactVectorSearchResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
	}, nil
}

func (index *Index) SimilarContacts(
	ctx context.Context,
	request contracts.SimilarContactsRequest,
) (contracts.SimilarContactsResponse, error) {
	contactID, err := index.resolveContactRef(ctx, request.ContactID)
	if err != nil {
		return contracts.SimilarContactsResponse{}, err
	}
	if contactID == "" {
		return contracts.SimilarContactsResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
		}, nil
	}

	args := []any{contactID}
	seedWhere := []string{
		"contact_id = $1",
		"embedding IS NOT NULL",
	}
	where := []string{"candidate.embedding IS NOT NULL"}
	if request.Model != "" {
		args = append(args, request.Model)
		seedWhere = append(seedWhere, fmt.Sprintf("model = $%d", len(args)))
		where = append(where, fmt.Sprintf("candidate.model = $%d", len(args)))
	}

	args = append(args, normalizedLimit(request.Limit))
	sqlText := fmt.Sprintf(
		"WITH seed AS (\n"+
			"  SELECT model, dimensions, embedding\n"+
			"    FROM query_contact_embeddings\n"+
			"   WHERE %s\n"+
			"   ORDER BY embedding_id\n"+
			"   LIMIT 1\n"+
			")\n"+
			"SELECT best.contact_id, COALESCE(r.display_name, ''), COALESCE(r.primary_email, ''),\n"+
			"       best.model, best.embedding_id, best.input_hash, best.text_preview,\n"+
			"       best.dimensions, best.distance\n"+
			"  FROM (\n"+
			"        SELECT DISTINCT ON (ranked.contact_id)\n"+
			"               ranked.contact_id, ranked.model, ranked.embedding_id,\n"+
			"               ranked.input_hash, ranked.text_preview, ranked.dimensions,\n"+
			"               ranked.distance\n"+
			"          FROM (\n"+
			"                SELECT candidate.contact_id, candidate.model,\n"+
			"                       candidate.embedding_id, candidate.input_hash,\n"+
			"                       candidate.text_preview, candidate.dimensions,\n"+
			"                       candidate.embedding <=> seed.embedding AS distance\n"+
			"                  FROM query_contact_embeddings candidate\n"+
			"                  JOIN seed ON seed.model = candidate.model\n"+
			"                   AND seed.dimensions = candidate.dimensions\n"+
			"                 WHERE candidate.contact_id <> $1\n"+
			"                   AND %s\n"+
			"               ) ranked\n"+
			"         ORDER BY ranked.contact_id, ranked.distance\n"+
			"       ) best\n"+
			"  LEFT JOIN query_contact_rollups r ON r.contact_id = best.contact_id\n"+
			" ORDER BY best.distance, best.contact_id\n"+
			" LIMIT $%d",
		strings.Join(seedWhere, " AND "),
		strings.Join(where, " AND "),
		len(args),
	)
	rows, err := index.pool.Query(ctx, sqlText, args...)
	if err != nil {
		return contracts.SimilarContactsResponse{}, fmt.Errorf(
			"similar contacts: %w",
			err,
		)
	}
	defer rows.Close()

	results, err := scanContactVectorSearchRows(rows)
	if err != nil {
		return contracts.SimilarContactsResponse{}, err
	}

	return contracts.SimilarContactsResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
	}, nil
}

func scanContactVectorSearchRows(
	rows pgx.Rows,
) ([]contracts.ContactVectorSearchResult, error) {
	results := []contracts.ContactVectorSearchResult{}
	for rows.Next() {
		var result contracts.ContactVectorSearchResult
		err := rows.Scan(
			&result.ContactID,
			&result.DisplayName,
			&result.PrimaryEmail,
			&result.Model,
			&result.EmbeddingID,
			&result.InputHash,
			&result.TextPreview,
			&result.Dimensions,
			&result.Distance,
		)
		if err != nil {
			return nil, fmt.Errorf("scan contact vector search: %w", err)
		}
		results = append(results, result)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contact vector search: %w", err)
	}

	return results, nil
}
