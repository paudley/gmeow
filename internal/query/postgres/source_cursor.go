// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"blackcat.ca/gmeow/internal/contracts"
)

func (index *Index) ProjectSourceCursor(
	ctx context.Context,
	cursor contracts.SourceCursor,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	encoded, err := json.Marshal(nonNilMap(cursor.Cursor))
	if err != nil {
		return err
	}

	if cursor.UpdatedAt.IsZero() {
		cursor.UpdatedAt = time.Now().UTC()
	}

	tx, err := index.pool.Begin(ctx)
	if err != nil {
		return fmt.Errorf("begin source cursor projection: %w", err)
	}
	defer tx.Rollback(ctx)

	if _, err := tx.Exec(
		ctx,
		`INSERT INTO query_source_cursors(source_name, source_kind, cursor_json, updated_at)
		 VALUES($1,$2,$3,$4)
		 ON CONFLICT(source_name) DO UPDATE SET
		   source_kind = excluded.source_kind,
		   cursor_json = excluded.cursor_json,
		   updated_at = excluded.updated_at`,
		cursor.SourceName,
		cursor.SourceKind,
		encoded,
		cursor.UpdatedAt,
	); err != nil {
		return fmt.Errorf("project source cursor %s: %w", cursor.SourceName, err)
	}

	mailboxes, ok, err := jmapMailboxCatalogFromSourceCursor(cursor)
	if err != nil {
		return fmt.Errorf("parse JMAP mailbox catalog cursor: %w", err)
	}
	if ok {
		if err := projectJMAPMailboxCatalogTx(ctx, tx, mailboxes, false); err != nil {
			return err
		}
	}
	if err := tx.Commit(ctx); err != nil {
		return fmt.Errorf("commit source cursor projection: %w", err)
	}

	return nil
}
