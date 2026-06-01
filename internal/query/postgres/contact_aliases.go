// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"errors"
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
		   FROM active_contact_aliases
		  WHERE contact_alias = $1
		  LIMIT 1`,
		alias,
	).Scan(&contactID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
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
		 ) OR EXISTS(
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
	inputs := uniqueNonEmptyStrings(values)
	if len(inputs) == 0 {
		return nil, nil
	}

	existing, err := index.existingContactRefs(ctx, inputs)
	if err != nil {
		return nil, err
	}

	aliases := make([]string, 0, len(inputs))
	aliasByInput := map[string]string{}
	for _, value := range inputs {
		if existing[value] {
			continue
		}

		alias := contactentity.NormalizeAlias(value)
		if alias == "" {
			continue
		}

		aliasByInput[value] = alias
		aliases = append(aliases, alias)
	}

	resolvedAliases, err := index.resolveActiveContactAliases(ctx, aliases)
	if err != nil {
		return nil, err
	}

	resolved := make([]string, 0, len(inputs))
	for _, value := range inputs {
		if existing[value] {
			resolved = append(resolved, value)
			continue
		}

		alias := aliasByInput[value]
		if contactID := resolvedAliases[alias]; contactID != "" {
			resolved = append(resolved, contactID)
			continue
		}

		resolved = append(resolved, value)
	}

	return uniqueNonEmptyStrings(resolved), nil
}

func (index *Index) existingContactRefs(
	ctx context.Context,
	contactIDs []string,
) (map[string]bool, error) {
	contacts := uniqueNonEmptyStrings(contactIDs)
	if len(contacts) == 0 {
		return map[string]bool{}, nil
	}

	rows, err := index.pool.Query(
		ctx,
		`SELECT contact_id FROM query_contact_rollups WHERE contact_id = ANY($1)
		 UNION
		 SELECT contact_id FROM query_contact_facts WHERE contact_id = ANY($1)`,
		contacts,
	)
	if err != nil {
		return nil, fmt.Errorf("batch check contact existence: %w", err)
	}
	defer rows.Close()

	existing := map[string]bool{}
	for rows.Next() {
		var contactID string
		if err := rows.Scan(&contactID); err != nil {
			return nil, fmt.Errorf("scan existing contact: %w", err)
		}
		existing[contactID] = true
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate existing contacts: %w", err)
	}

	return existing, nil
}

func (index *Index) resolveActiveContactAliases(
	ctx context.Context,
	aliases []string,
) (map[string]string, error) {
	aliases = uniqueNonEmptyStrings(aliases)
	if len(aliases) == 0 {
		return map[string]string{}, nil
	}

	rows, err := index.pool.Query(
		ctx,
		`SELECT contact_alias, contact_id
		   FROM active_contact_aliases
		  WHERE contact_alias = ANY($1)
		  ORDER BY contact_alias`,
		aliases,
	)
	if err != nil {
		return nil, fmt.Errorf("batch resolve contact aliases: %w", err)
	}
	defer rows.Close()

	resolved := map[string]string{}
	for rows.Next() {
		var alias string
		var contactID string
		if err := rows.Scan(&alias, &contactID); err != nil {
			return nil, fmt.Errorf("scan active contact alias: %w", err)
		}
		resolved[alias] = contactID
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("iterate active contact aliases: %w", err)
	}

	return resolved, nil
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
		`SELECT DISTINCT contact_id, contact_alias
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
