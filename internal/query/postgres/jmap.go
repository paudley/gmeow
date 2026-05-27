// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"fmt"
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
	jmapMailboxTrash   = "trash"
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
		       COALESCE(array_agg(DISTINCT m.mailbox_id) FILTER (WHERE m.mailbox_id IS NOT NULL), '{}') AS mailbox_ids,
		       COALESCE(array_agg(DISTINCT k.keyword) FILTER (WHERE k.keyword IS NOT NULL), '{}') AS keywords
		  FROM jmap_email_state s
		  LEFT JOIN jmap_email_mailboxes m ON m.object_digest = s.object_digest
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
	for _, mailboxID := range mailboxIDs {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_email_mailboxes(object_digest, mailbox_id)
			VALUES($1, $2)`,
			update.ObjectDigest,
			mailboxID,
		); err != nil {
			return contracts.JMAPEmailState{}, fmt.Errorf("insert JMAP mailbox: %w", err)
		}
	}

	if _, err := tx.Exec(
		ctx,
		"DELETE FROM jmap_email_keywords WHERE object_digest = $1",
		update.ObjectDigest,
	); err != nil {
		return contracts.JMAPEmailState{}, fmt.Errorf("clear JMAP keywords: %w", err)
	}
	for _, keyword := range keywords {
		if _, err := tx.Exec(ctx, `
			INSERT INTO jmap_email_keywords(object_digest, keyword)
			VALUES($1, $2)`,
			update.ObjectDigest,
			keyword,
		); err != nil {
			return contracts.JMAPEmailState{}, fmt.Errorf("insert JMAP keyword: %w", err)
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
		INSERT INTO jmap_email_state(object_digest, thread_id)
		VALUES($1, $2)
		ON CONFLICT(object_digest) DO UPDATE SET
		  thread_id = excluded.thread_id,
		  updated_at = now()`,
		manifest.ObjectDigest,
		stringFromAny(metadata["thread_id"]),
	); err != nil {
		return fmt.Errorf("seed JMAP email state: %w", err)
	}

	labelIDs := labelIDsFromMetadata(metadata)
	if overlay, ok := jmapOverlayState(manifest, annotations); ok {
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
