// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"fmt"
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

func seedJMAPEmailStateTx(
	ctx context.Context,
	tx pgx.Tx,
	manifest contracts.Manifest,
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
