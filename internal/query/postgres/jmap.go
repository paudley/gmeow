// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/mail"
	"sort"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
)

const (
	jmapMailboxAll     = "all"
	jmapMailboxArchive = "archive"
	jmapMailboxDrafts  = "drafts"
	jmapMailboxInbox   = "inbox"
	jmapMailboxSent    = "sent"
	jmapMailboxSpam    = "spam"
	jmapMailboxStarred = "starred"
	jmapMailboxTrash   = "trash"
	jmapMailboxUnread  = "unread"
	jmapKeywordFlagged = "$flagged"
	jmapKeywordSeen    = "$seen"
)

const jmapOverlayKey = "jmap"

func (index *Index) JMAPMailboxes(
	ctx context.Context,
) ([]contracts.JMAPMailbox, error) {
	rows, err := index.pool.Query(ctx, `
		SELECT mailbox_id, name, role, parent_id, sort_order, is_system,
		       is_destroyed, created_at, updated_at
		  FROM jmap_mailboxes
		 WHERE is_destroyed = false
		 ORDER BY sort_order, name, mailbox_id`)
	if err != nil {
		return nil, fmt.Errorf("query JMAP mailboxes: %w", err)
	}
	defer rows.Close()

	var mailboxes []contracts.JMAPMailbox
	for rows.Next() {
		var mailbox contracts.JMAPMailbox
		if err := rows.Scan(
			&mailbox.MailboxID,
			&mailbox.Name,
			&mailbox.Role,
			&mailbox.ParentID,
			&mailbox.SortOrder,
			&mailbox.IsSystem,
			&mailbox.IsDestroyed,
			&mailbox.CreatedAt,
			&mailbox.UpdatedAt,
		); err != nil {
			return nil, fmt.Errorf("scan JMAP mailbox: %w", err)
		}
		mailboxes = append(mailboxes, mailbox)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate JMAP mailboxes: %w", err)
	}

	return mailboxes, nil
}

func (index *Index) UpdateJMAPMailboxCatalog(
	ctx context.Context,
	update contracts.JMAPMailboxCatalogUpdate,
) ([]contracts.JMAPMailbox, error) {
	tx, err := index.pool.Begin(ctx)
	if err != nil {
		return nil, fmt.Errorf("begin JMAP mailbox catalog update: %w", err)
	}
	defer tx.Rollback(ctx)

	if err := projectJMAPMailboxCatalogTx(ctx, tx, update.Mailboxes, true); err != nil {
		return nil, err
	}
	if err := tx.Commit(ctx); err != nil {
		return nil, fmt.Errorf("commit JMAP mailbox catalog update: %w", err)
	}

	return index.JMAPMailboxes(ctx)
}

func (index *Index) JMAPMailboxEmailCounts(
	ctx context.Context,
	request contracts.JMAPMailboxEmailCountRequest,
) (contracts.JMAPMailboxEmailCountResponse, error) {
	mailboxIDs := uniqueSortedNonEmpty(request.MailboxIDs)
	counts := make(map[string]int, len(mailboxIDs))
	for _, mailboxID := range mailboxIDs {
		counts[mailboxID] = 0
	}
	if len(mailboxIDs) == 0 {
		return contracts.JMAPMailboxEmailCountResponse{Counts: counts}, nil
	}

	rows, err := index.pool.Query(ctx, `
		SELECT mailbox_id, count(*)
		  FROM jmap_email_mailboxes
		 WHERE mailbox_id = ANY($1::text[])
		 GROUP BY mailbox_id`,
		mailboxIDs)
	if err != nil {
		return contracts.JMAPMailboxEmailCountResponse{}, fmt.Errorf(
			"query JMAP mailbox email counts: %w",
			err,
		)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			mailboxID string
			count     int64
		)
		if err := rows.Scan(&mailboxID, &count); err != nil {
			return contracts.JMAPMailboxEmailCountResponse{}, fmt.Errorf(
				"scan JMAP mailbox email count: %w",
				err,
			)
		}
		counts[mailboxID] = int(count)
	}
	if err := rows.Err(); err != nil {
		return contracts.JMAPMailboxEmailCountResponse{}, fmt.Errorf(
			"iterate JMAP mailbox email counts: %w",
			err,
		)
	}

	return contracts.JMAPMailboxEmailCountResponse{Counts: counts}, nil
}

func (index *Index) JMAPEmailStates(
	ctx context.Context,
	digests []contracts.ObjectDigest,
) (map[contracts.ObjectDigest]contracts.JMAPEmailState, error) {
	if len(digests) == 0 {
		return map[contracts.ObjectDigest]contracts.JMAPEmailState{}, nil
	}

	values := make([]string, 0, len(digests))
	for _, digest := range digests {
		values = append(values, string(digest))
	}

	rows, err := index.pool.Query(ctx, `
		SELECT s.object_digest, s.thread_id, s.received_at, s.state_seq,
		       COALESCE(array_agg(DISTINCT m.mailbox_id) FILTER (WHERE m.mailbox_id IS NOT NULL AND mb.mailbox_id IS NOT NULL), '{}') AS mailbox_ids,
		       COALESCE(array_agg(DISTINCT k.keyword) FILTER (WHERE k.keyword IS NOT NULL), '{}') AS keywords
		  FROM jmap_email_state s
		  LEFT JOIN jmap_email_mailboxes m ON m.object_digest = s.object_digest
		  LEFT JOIN jmap_mailboxes mb ON mb.mailbox_id = m.mailbox_id
		   AND mb.is_destroyed = false
		  LEFT JOIN jmap_email_keywords k ON k.object_digest = s.object_digest
		 WHERE s.object_digest = ANY($1::text[])
		 GROUP BY s.object_digest, s.thread_id, s.received_at, s.state_seq`,
		values)
	if err != nil {
		return nil, fmt.Errorf("query JMAP email states: %w", err)
	}
	defer rows.Close()

	states := make(map[contracts.ObjectDigest]contracts.JMAPEmailState)
	for rows.Next() {
		var (
			state      contracts.JMAPEmailState
			receivedAt *time.Time
		)
		if err := rows.Scan(
			&state.ObjectDigest,
			&state.ThreadID,
			&receivedAt,
			&state.StateSequence,
			&state.MailboxIDs,
			&state.Keywords,
		); err != nil {
			return nil, fmt.Errorf("scan JMAP email state: %w", err)
		}
		if receivedAt != nil {
			state.ReceivedAt = *receivedAt
		}
		states[state.ObjectDigest] = state
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate JMAP email states: %w", err)
	}

	return states, nil
}

func (index *Index) JMAPEmailQuery(
	ctx context.Context,
	request contracts.JMAPEmailQueryRequest,
) (contracts.JMAPEmailQueryResponse, error) {
	args := []any{}
	where := []string{"true"}
	if request.Text != "" {
		args = append(args, request.Text)
		where = append(
			where,
			fmt.Sprintf("o.search_tsv @@ websearch_to_tsquery('simple', $%d)", len(args)),
		)
	}
	if request.InMailbox != "" {
		args = append(args, request.InMailbox)
		where = append(where, fmt.Sprintf(`
			EXISTS (
				SELECT 1 FROM jmap_email_mailboxes m
				  JOIN jmap_mailboxes mb ON mb.mailbox_id = m.mailbox_id
				   AND mb.is_destroyed = false
				 WHERE m.object_digest = s.object_digest
				   AND m.mailbox_id = $%d
			)`, len(args)))
	}
	if request.HasKeyword != "" {
		args = append(args, request.HasKeyword)
		where = append(where, fmt.Sprintf(`
			EXISTS (
				SELECT 1 FROM jmap_email_keywords k
				 WHERE k.object_digest = s.object_digest
				   AND k.keyword = $%d
			)`, len(args)))
	}
	if request.NotKeyword != "" {
		args = append(args, request.NotKeyword)
		where = append(where, fmt.Sprintf(`
			NOT EXISTS (
				SELECT 1 FROM jmap_email_keywords k
				 WHERE k.object_digest = s.object_digest
				   AND k.keyword = $%d
			)`, len(args)))
	}
	if !request.After.IsZero() {
		args = append(args, request.After)
		where = append(where, fmt.Sprintf("s.received_at >= $%d", len(args)))
	}
	if !request.Before.IsZero() {
		args = append(args, request.Before)
		where = append(where, fmt.Sprintf("s.received_at < $%d", len(args)))
	}

	limit := normalizedLimit(request.Limit)
	offset := request.Offset
	if offset < 0 {
		offset = 0
	}
	args = append(args, limit, offset)
	rows, err := index.pool.Query(ctx, fmt.Sprintf(`
		SELECT s.object_digest, COUNT(*) OVER() AS total
		  FROM jmap_email_state s
		  JOIN query_objects o ON o.object_digest = s.object_digest
		 WHERE %s
		 ORDER BY s.received_at DESC NULLS LAST, o.updated_at DESC, s.object_digest
		 LIMIT $%d OFFSET $%d`,
		strings.Join(where, " AND "),
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return contracts.JMAPEmailQueryResponse{}, fmt.Errorf("query JMAP emails: %w", err)
	}
	defer rows.Close()

	ids := []contracts.ObjectDigest{}
	total := 0
	for rows.Next() {
		var id contracts.ObjectDigest
		if err := rows.Scan(&id, &total); err != nil {
			return contracts.JMAPEmailQueryResponse{}, fmt.Errorf("scan JMAP email: %w", err)
		}
		ids = append(ids, id)
	}
	if err := rows.Err(); err != nil {
		return contracts.JMAPEmailQueryResponse{}, fmt.Errorf("iterate JMAP emails: %w", err)
	}

	return contracts.JMAPEmailQueryResponse{
		IDs:    ids,
		Total:  total,
		Offset: offset,
		Limit:  limit,
	}, nil
}

func (index *Index) JMAPThreads(
	ctx context.Context,
	ids []string,
) (map[string]contracts.JMAPThread, error) {
	if len(ids) == 0 {
		return map[string]contracts.JMAPThread{}, nil
	}

	threadIDs := uniqueSortedNonEmpty(ids)
	rows, err := index.pool.Query(ctx, `
		SELECT s.thread_id,
		       array_agg(s.object_digest ORDER BY s.received_at ASC NULLS LAST, o.created_at ASC, s.object_digest) AS email_ids
		  FROM jmap_email_state s
		  JOIN query_objects o ON o.object_digest = s.object_digest
		 WHERE s.thread_id = ANY($1::text[])
		 GROUP BY s.thread_id`,
		threadIDs)
	if err != nil {
		return nil, fmt.Errorf("query JMAP threads: %w", err)
	}
	defer rows.Close()

	threads := make(map[string]contracts.JMAPThread)
	for rows.Next() {
		var (
			threadID string
			emailIDs []contracts.ObjectDigest
		)
		if err := rows.Scan(&threadID, &emailIDs); err != nil {
			return nil, fmt.Errorf("scan JMAP thread: %w", err)
		}
		threads[threadID] = contracts.JMAPThread{
			ID:       threadID,
			EmailIDs: append([]contracts.ObjectDigest{}, emailIDs...),
		}
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate JMAP threads: %w", err)
	}

	return threads, nil
}

func (index *Index) JMAPBlobLookup(
	ctx context.Context,
	request contracts.JMAPBlobLookupRequest,
) (contracts.JMAPBlobLookupResponse, error) {
	blobIDs := uniqueSortedObjectDigests(request.BlobIDs)
	blobs := make(map[contracts.ObjectDigest]contracts.JMAPBlobReferences, len(blobIDs))
	for _, blobID := range blobIDs {
		blobs[blobID] = contracts.JMAPBlobReferences{}
	}
	if len(blobIDs) == 0 {
		return contracts.JMAPBlobLookupResponse{Blobs: blobs}, nil
	}

	values := make([]string, 0, len(blobIDs))
	for _, blobID := range blobIDs {
		values = append(values, string(blobID))
	}
	rows, err := index.pool.Query(ctx, `
		WITH RECURSIVE input(blob_id) AS (
			SELECT unnest($1::text[])
		), walk(blob_id, current_digest) AS (
			SELECT blob_id, blob_id FROM input
			UNION
			SELECT walk.blob_id, c.object_digest
			  FROM walk
			  JOIN query_object_compound_parts c
			    ON c.part_digest = walk.current_digest
		), emails AS (
			SELECT DISTINCT walk.blob_id, s.object_digest AS email_id, s.thread_id
			  FROM walk
			  JOIN jmap_email_state s ON s.object_digest = walk.current_digest
		)
		SELECT e.blob_id, e.email_id, e.thread_id,
		       COALESCE(array_agg(DISTINCT m.mailbox_id) FILTER (WHERE m.mailbox_id IS NOT NULL), '{}') AS mailbox_ids
		  FROM emails e
		  LEFT JOIN jmap_email_mailboxes m ON m.object_digest = e.email_id
		 GROUP BY e.blob_id, e.email_id, e.thread_id
		 ORDER BY e.blob_id, e.email_id`,
		values)
	if err != nil {
		return contracts.JMAPBlobLookupResponse{}, fmt.Errorf(
			"query JMAP blob lookup: %w",
			err,
		)
	}
	defer rows.Close()

	for rows.Next() {
		var (
			blobID     contracts.ObjectDigest
			emailID    contracts.ObjectDigest
			threadID   string
			mailboxIDs []string
		)
		if err := rows.Scan(&blobID, &emailID, &threadID, &mailboxIDs); err != nil {
			return contracts.JMAPBlobLookupResponse{}, fmt.Errorf(
				"scan JMAP blob lookup: %w",
				err,
			)
		}
		references := blobs[blobID]
		references.EmailIDs = appendUniqueObjectDigest(references.EmailIDs, emailID)
		references.ThreadIDs = appendUniqueString(references.ThreadIDs, threadID)
		for _, mailboxID := range mailboxIDs {
			references.MailboxIDs = appendUniqueString(references.MailboxIDs, mailboxID)
		}
		blobs[blobID] = references
	}
	if err := rows.Err(); err != nil {
		return contracts.JMAPBlobLookupResponse{}, fmt.Errorf(
			"iterate JMAP blob lookup: %w",
			err,
		)
	}

	return contracts.JMAPBlobLookupResponse{Blobs: blobs}, nil
}

func (index *Index) UpdateJMAPEmailState(
	ctx context.Context,
	update contracts.JMAPEmailStateUpdate,
) (contracts.JMAPEmailState, error) {
	tx, err := index.pool.Begin(ctx)
	if err != nil {
		return contracts.JMAPEmailState{}, fmt.Errorf("begin JMAP email update: %w", err)
	}
	defer tx.Rollback(ctx)

	state, err := applyJMAPEmailStateTx(ctx, tx, update, true)
	if err != nil {
		return contracts.JMAPEmailState{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return contracts.JMAPEmailState{}, fmt.Errorf("commit JMAP email update: %w", err)
	}

	return state, nil
}

func applyJMAPEmailStateTx(
	ctx context.Context,
	tx pgx.Tx,
	update contracts.JMAPEmailStateUpdate,
	incrementSequence bool,
) (contracts.JMAPEmailState, error) {
	mailboxIDs := uniqueSortedNonEmpty(update.MailboxIDs)
	keywords := uniqueSortedNonEmpty(update.Keywords)
	if len(mailboxIDs) == 0 {
		return contracts.JMAPEmailState{}, fmt.Errorf(
			"JMAP email %s must have at least one mailbox",
			update.ObjectDigest,
		)
	}

	var (
		threadID string
		sequence int64
	)
	sequenceSQL := "state_seq"
	if incrementSequence {
		sequenceSQL = "state_seq + 1"
	}
	if err := tx.QueryRow(ctx, fmt.Sprintf(`
		UPDATE jmap_email_state
		   SET state_seq = %s,
		       updated_at = now()
		 WHERE object_digest = $1
		 RETURNING thread_id, state_seq`, sequenceSQL),
		update.ObjectDigest,
	).Scan(&threadID, &sequence); err != nil {
		return contracts.JMAPEmailState{}, fmt.Errorf("update JMAP email state: %w", err)
	}

	if _, err := tx.Exec(
		ctx,
		"DELETE FROM jmap_email_mailboxes WHERE object_digest = $1",
		update.ObjectDigest,
	); err != nil {
		return contracts.JMAPEmailState{}, fmt.Errorf("clear JMAP mailboxes: %w", err)
	}
	if len(mailboxIDs) != 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_email_mailboxes(object_digest, mailbox_id)
			SELECT $1, unnest($2::text[])`,
			update.ObjectDigest,
			mailboxIDs,
		); err != nil {
			return contracts.JMAPEmailState{}, fmt.Errorf("insert JMAP mailboxes: %w", err)
		}
	}

	if _, err := tx.Exec(
		ctx,
		"DELETE FROM jmap_email_keywords WHERE object_digest = $1",
		update.ObjectDigest,
	); err != nil {
		return contracts.JMAPEmailState{}, fmt.Errorf("clear JMAP keywords: %w", err)
	}
	if len(keywords) != 0 {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_email_keywords(object_digest, keyword)
			SELECT $1, unnest($2::text[])`,
			update.ObjectDigest,
			keywords,
		); err != nil {
			return contracts.JMAPEmailState{}, fmt.Errorf("insert JMAP keywords: %w", err)
		}
	}

	return contracts.JMAPEmailState{
		ObjectDigest:  update.ObjectDigest,
		ThreadID:      threadID,
		MailboxIDs:    mailboxIDs,
		Keywords:      keywords,
		StateSequence: sequence,
	}, nil
}

func seedJMAPEmailStateTx(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) error {
	metadata, ok := mailMessageFacetMetadata(manifest)
	if !ok {
		return nil
	}

	if _, err := tx.Exec(ctx, `
		INSERT INTO jmap_email_state(object_digest, thread_id, received_at)
		VALUES($1, $2, $3)
		ON CONFLICT(object_digest) DO UPDATE SET
		  thread_id = excluded.thread_id,
		  received_at = coalesce(jmap_email_state.received_at, excluded.received_at),
		  updated_at = now()`,
		manifest.ObjectDigest,
		stringFromAny(metadata["thread_id"]),
		jmapReceivedAt(manifest, metadata),
	); err != nil {
		return fmt.Errorf("seed JMAP email state: %w", err)
	}

	labelIDs := labelIDsFromMetadata(metadata)
	if overlay, ok := jmapOverlayState(
		manifest,
		annotations,
	); ok &&
		len(overlay.MailboxIDs) != 0 {
		_, err := applyJMAPEmailStateTx(ctx, tx, contracts.JMAPEmailStateUpdate{
			ObjectDigest: manifest.ObjectDigest,
			MailboxIDs:   overlay.MailboxIDs,
			Keywords:     overlay.Keywords,
		}, false)
		return err
	}

	if err := seedJMAPEmailMailboxesTx(
		ctx,
		tx,
		manifest.ObjectDigest,
		labelIDs,
	); err != nil {
		return err
	}
	if err := seedJMAPEmailKeywordsTx(
		ctx,
		tx,
		manifest.ObjectDigest,
		labelIDs,
	); err != nil {
		return err
	}

	return nil
}

func seedJMAPEmailMailboxesTx(
	ctx context.Context,
	tx pgx.Tx,
	digest contracts.ObjectDigest,
	labelIDs []string,
) error {
	var existingMailboxCount int
	if err := tx.QueryRow(
		ctx,
		"SELECT count(*) FROM jmap_email_mailboxes WHERE object_digest = $1",
		digest,
	).Scan(&existingMailboxCount); err != nil {
		return fmt.Errorf("count JMAP mailboxes for email: %w", err)
	}
	if existingMailboxCount != 0 {
		return nil
	}

	for _, mailboxID := range defaultJMAPMailboxes(labelIDs) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_email_mailboxes(object_digest, mailbox_id)
			VALUES($1, $2)
			ON CONFLICT(object_digest, mailbox_id) DO NOTHING`,
			digest,
			mailboxID,
		); err != nil {
			return fmt.Errorf("seed JMAP mailbox membership: %w", err)
		}
	}

	return nil
}

func seedJMAPEmailKeywordsTx(
	ctx context.Context,
	tx pgx.Tx,
	digest contracts.ObjectDigest,
	labelIDs []string,
) error {
	var existingKeywordCount int
	if err := tx.QueryRow(
		ctx,
		"SELECT count(*) FROM jmap_email_keywords WHERE object_digest = $1",
		digest,
	).Scan(&existingKeywordCount); err != nil {
		return fmt.Errorf("count JMAP keywords for email: %w", err)
	}
	if existingKeywordCount != 0 {
		return nil
	}

	for _, keyword := range defaultJMAPKeywords(labelIDs) {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_email_keywords(object_digest, keyword)
			VALUES($1, $2)
			ON CONFLICT(object_digest, keyword) DO NOTHING`,
			digest,
			keyword,
		); err != nil {
			return fmt.Errorf("seed JMAP keyword: %w", err)
		}
	}

	return nil
}

func mailMessageFacetMetadata(manifest contracts.Manifest) (map[string]any, bool) {
	for _, facet := range manifest.Facets {
		if facet.FacetKind() == "mail_message" {
			return firstMap(facet.Metadata, facet.Attributes), true
		}
	}

	return nil, false
}

func defaultJMAPMailboxes(labelIDs []string) []string {
	mailboxes := []string{jmapMailboxAll}
	switch {
	case containsString(labelIDs, "TRASH"):
		mailboxes = append(mailboxes, jmapMailboxTrash)
	case containsString(labelIDs, "SPAM"):
		mailboxes = append(mailboxes, jmapMailboxSpam)
	case containsString(labelIDs, "SENT"):
		mailboxes = append(mailboxes, jmapMailboxSent)
	case containsString(labelIDs, "DRAFT"):
		mailboxes = append(mailboxes, jmapMailboxDrafts)
	case containsString(labelIDs, "INBOX"):
		mailboxes = append(mailboxes, jmapMailboxInbox)
	default:
		mailboxes = append(mailboxes, jmapMailboxArchive)
	}
	return mailboxes
}

func jmapReceivedAt(manifest contracts.Manifest, metadata map[string]any) *time.Time {
	for _, key := range []string{"received_at", "date"} {
		if receivedAt, ok := parseJMAPTime(stringFromAny(metadata[key])); ok {
			return &receivedAt
		}
	}
	for _, candidate := range []time.Time{
		manifest.Timestamps.Observed,
		manifest.Timestamps.Modified,
		manifest.CreatedAt,
	} {
		if !candidate.IsZero() {
			receivedAt := candidate.UTC()

			return &receivedAt
		}
	}

	return nil
}

func parseJMAPTime(value string) (time.Time, bool) {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return time.Time{}, false
	}
	if parsed, err := time.Parse(time.RFC3339Nano, trimmed); err == nil {
		return parsed.UTC(), true
	}
	if parsed, err := mail.ParseDate(trimmed); err == nil {
		return parsed.UTC(), true
	}

	return time.Time{}, false
}

func defaultJMAPKeywords(labelIDs []string) []string {
	keywords := []string{}
	if !containsString(labelIDs, "UNREAD") {
		keywords = append(keywords, jmapKeywordSeen)
	}
	if containsString(labelIDs, "STARRED") {
		keywords = append(keywords, jmapKeywordFlagged)
	}

	return keywords
}

func labelIDsFromMetadata(metadata map[string]any) []string {
	switch labels := metadata["label_ids"].(type) {
	case []string:
		return append([]string{}, labels...)
	case []any:
		values := make([]string, 0, len(labels))
		for _, label := range labels {
			if value := stringFromAny(label); value != "" {
				values = append(values, value)
			}
		}
		return values
	default:
		return nil
	}
}

func jmapOverlayState(
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) (contracts.JMAPEmailStateUpdate, bool) {
	for _, annotation := range annotations {
		if annotation.Kind == "overlays" {
			if state, ok := jmapOverlayStateFromMap(
				annotation.Data,
				manifest.ObjectDigest,
			); ok {
				return state, true
			}
		}
	}

	return jmapOverlayStateFromMap(manifest.Overlays, manifest.ObjectDigest)
}

func jmapOverlayStateFromMap(
	overlays map[string]any,
	digest contracts.ObjectDigest,
) (contracts.JMAPEmailStateUpdate, bool) {
	raw, ok := overlays[jmapOverlayKey]
	if !ok {
		return contracts.JMAPEmailStateUpdate{}, false
	}
	overlay, ok := raw.(map[string]any)
	if !ok {
		return contracts.JMAPEmailStateUpdate{}, false
	}

	return contracts.JMAPEmailStateUpdate{
		ObjectDigest: digest,
		MailboxIDs:   stringListFromAny(overlay["mailbox_ids"]),
		Keywords:     stringListFromAny(overlay["keywords"]),
	}, true
}

func stringListFromAny(value any) []string {
	switch typed := value.(type) {
	case []string:
		return append([]string{}, typed...)
	case []any:
		values := make([]string, 0, len(typed))
		for _, item := range typed {
			if text := stringFromAny(item); text != "" {
				values = append(values, text)
			}
		}
		return values
	default:
		return nil
	}
}

func uniqueSortedNonEmpty(values []string) []string {
	seen := map[string]bool{}
	out := []string{}
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Strings(out)

	return out
}

func uniqueSortedObjectDigests(
	values []contracts.ObjectDigest,
) []contracts.ObjectDigest {
	seen := map[contracts.ObjectDigest]bool{}
	out := []contracts.ObjectDigest{}
	for _, value := range values {
		if value == "" || seen[value] {
			continue
		}
		seen[value] = true
		out = append(out, value)
	}
	sort.Slice(out, func(left, right int) bool {
		return out[left] < out[right]
	})

	return out
}

func appendUniqueObjectDigest(
	values []contracts.ObjectDigest,
	value contracts.ObjectDigest,
) []contracts.ObjectDigest {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}

	return append(values, value)
}

func appendUniqueString(values []string, value string) []string {
	if value == "" {
		return values
	}
	for _, existing := range values {
		if existing == value {
			return values
		}
	}

	return append(values, value)
}

func projectJMAPMailboxCatalogTx(
	ctx context.Context,
	tx pgx.Tx,
	mailboxes []contracts.JMAPMailbox,
	incrementSequence bool,
) error {
	catalog, err := normalizedJMAPMailboxCatalog(mailboxes)
	if err != nil {
		return err
	}
	ids := make([]string, 0, len(catalog))
	for _, mailbox := range catalog {
		ids = append(ids, mailbox.MailboxID)
	}
	if _, err := tx.Exec(ctx, `
		UPDATE jmap_mailboxes
		   SET is_destroyed = true, updated_at = now()
		 WHERE is_system = false
		   AND NOT (mailbox_id = ANY($1::text[]))`,
		ids,
	); err != nil {
		return fmt.Errorf("destroy removed JMAP mailboxes: %w", err)
	}
	for _, mailbox := range catalog {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_mailboxes(
			  mailbox_id, name, role, parent_id, sort_order, is_system,
			  is_destroyed
			)
			VALUES($1, $2, '', $3, $4, false, false)
			ON CONFLICT(mailbox_id) DO UPDATE SET
			  name = excluded.name,
			  role = '',
			  parent_id = excluded.parent_id,
			  sort_order = excluded.sort_order,
			  is_system = false,
			  is_destroyed = false,
			  updated_at = now()`,
			mailbox.MailboxID,
			mailbox.Name,
			mailbox.ParentID,
			mailbox.SortOrder,
		); err != nil {
			return fmt.Errorf("upsert JMAP mailbox %s: %w", mailbox.MailboxID, err)
		}
	}
	if incrementSequence {
		if _, err := tx.Exec(ctx, `
			UPDATE jmap_state_seq
			   SET state_seq = state_seq + 1, updated_at = now()
			 WHERE datatype = 'Mailbox'`,
		); err != nil {
			return fmt.Errorf("advance JMAP mailbox state: %w", err)
		}
	}

	return nil
}

func normalizedJMAPMailboxCatalog(
	mailboxes []contracts.JMAPMailbox,
) ([]contracts.JMAPMailbox, error) {
	systemIDs := map[string]bool{
		jmapMailboxAll:     true,
		jmapMailboxArchive: true,
		jmapMailboxDrafts:  true,
		jmapMailboxInbox:   true,
		jmapMailboxSent:    true,
		jmapMailboxSpam:    true,
		jmapMailboxStarred: true,
		jmapMailboxTrash:   true,
		jmapMailboxUnread:  true,
	}
	seenIDs := map[string]bool{}
	namesByParent := map[string]map[string]bool{}
	catalog := make([]contracts.JMAPMailbox, 0, len(mailboxes))
	for _, mailbox := range mailboxes {
		mailbox.MailboxID = strings.TrimSpace(mailbox.MailboxID)
		mailbox.Name = strings.TrimSpace(mailbox.Name)
		mailbox.ParentID = strings.TrimSpace(mailbox.ParentID)
		mailbox.Role = ""
		mailbox.IsSystem = false
		mailbox.IsDestroyed = false
		if mailbox.MailboxID == "" {
			return nil, errors.New("JMAP mailbox id is required")
		}
		if systemIDs[mailbox.MailboxID] {
			return nil, fmt.Errorf(
				"JMAP mailbox catalog cannot replace system mailbox %q",
				mailbox.MailboxID,
			)
		}
		if seenIDs[mailbox.MailboxID] {
			return nil, fmt.Errorf("duplicate JMAP mailbox id %q", mailbox.MailboxID)
		}
		if mailbox.Name == "" {
			return nil, fmt.Errorf("JMAP mailbox %q name is required", mailbox.MailboxID)
		}
		if mailbox.ParentID == mailbox.MailboxID {
			return nil, fmt.Errorf(
				"JMAP mailbox %q cannot be its own parent",
				mailbox.MailboxID,
			)
		}
		if mailbox.ParentID != "" && !systemIDs[mailbox.ParentID] {
			parentSeen := false
			for _, candidate := range mailboxes {
				if strings.TrimSpace(candidate.MailboxID) == mailbox.ParentID {
					parentSeen = true
					break
				}
			}
			if !parentSeen {
				return nil, fmt.Errorf(
					"JMAP mailbox %q parent %q is not in catalog",
					mailbox.MailboxID,
					mailbox.ParentID,
				)
			}
		}
		if namesByParent[mailbox.ParentID] == nil {
			namesByParent[mailbox.ParentID] = map[string]bool{}
		}
		nameKey := strings.ToLower(mailbox.Name)
		if namesByParent[mailbox.ParentID][nameKey] {
			return nil, fmt.Errorf(
				"duplicate JMAP mailbox name %q under parent %q",
				mailbox.Name,
				mailbox.ParentID,
			)
		}
		namesByParent[mailbox.ParentID][nameKey] = true
		seenIDs[mailbox.MailboxID] = true
		catalog = append(catalog, mailbox)
	}
	sort.Slice(catalog, func(left, right int) bool {
		if catalog[left].SortOrder != catalog[right].SortOrder {
			return catalog[left].SortOrder < catalog[right].SortOrder
		}
		return catalog[left].MailboxID < catalog[right].MailboxID
	})

	return catalog, nil
}

func jmapMailboxCatalogFromSourceCursor(
	cursor contracts.SourceCursor,
) ([]contracts.JMAPMailbox, bool, error) {
	if cursor.SourceKind != contracts.JMAPMailboxCatalogSourceKind ||
		cursor.SourceName != contracts.JMAPMailboxCatalogSourceName {
		return nil, false, nil
	}
	raw := cursor.Cursor[contracts.JMAPMailboxCatalogCursorKey]
	if raw == nil {
		return []contracts.JMAPMailbox{}, true, nil
	}
	encoded, err := json.Marshal(raw)
	if err != nil {
		return nil, true, err
	}
	var mailboxes []contracts.JMAPMailbox
	if err := json.Unmarshal(encoded, &mailboxes); err != nil {
		return nil, true, err
	}

	return mailboxes, true, nil
}
