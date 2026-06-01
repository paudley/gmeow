// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/facets/contactentity"
)

func (index *Index) resolveContactRef(
	ctx context.Context,
	value string,
) (string, error) {
	value = strings.TrimSpace(value)
	if value == "" {
		return "", nil
	}

	found, err := index.contactExists(ctx, value)
	if err != nil {
		return "", err
	}
	if found {
		return value, nil
	}

	alias := contactentity.NormalizeAlias(value)
	if alias == "" {
		return value, nil
	}

	var contactID string
	err = index.pool.QueryRow(
		ctx,
		`SELECT contact_id
		   FROM query_contact_aliases
		  WHERE contact_alias = $1
		    AND valid_until = ''
		  ORDER BY contact_id
		  LIMIT 1`,
		alias,
	).Scan(&contactID)
	if err != nil {
		if err == pgx.ErrNoRows {
			return value, nil
		}

		return "", fmt.Errorf("resolve contact alias %q: %w", value, err)
	}

	return contactID, nil
}

func (index *Index) contactExists(ctx context.Context, contactID string) (bool, error) {
	var exists bool
	err := index.pool.QueryRow(
		ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM query_contact_rollups WHERE contact_id = $1
		   UNION ALL
		   SELECT 1 FROM query_contact_facts WHERE contact_id = $1
		 )`,
		contactID,
	).Scan(&exists)
	if err != nil {
		return false, fmt.Errorf("check contact existence: %w", err)
	}

	return exists, nil
}

func (index *Index) resolveContactRefs(
	ctx context.Context,
	values []string,
) ([]string, error) {
	resolved := make([]string, 0, len(values))
	for _, value := range values {
		contactID, err := index.resolveContactRef(ctx, value)
		if err != nil {
			return nil, err
		}
		if contactID != "" {
			resolved = append(resolved, contactID)
		}
	}

	return uniqueNonEmptyStrings(resolved), nil
}

func (index *Index) aliasesForContacts(
	ctx context.Context,
	contactIDs []string,
) (map[string][]string, error) {
	contacts := uniqueNonEmptyStrings(contactIDs)
	if len(contacts) == 0 {
		return map[string][]string{}, nil
	}

	rows, err := index.pool.Query(
		ctx,
		`SELECT contact_id, contact_alias
		   FROM query_contact_aliases
		  WHERE contact_id = ANY($1)
		    AND valid_until = ''
		  ORDER BY contact_id, contact_alias`,
		contacts,
	)
	if err != nil {
		return nil, fmt.Errorf("query contact aliases: %w", err)
	}
	defer rows.Close()

	aliases := map[string][]string{}
	for rows.Next() {
		var contactID string
		var alias string
		if err := rows.Scan(&contactID, &alias); err != nil {
			return nil, fmt.Errorf("scan contact alias: %w", err)
		}
		aliases[contactID] = append(aliases[contactID], alias)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate contact aliases: %w", err)
	}

	return aliases, nil
}
