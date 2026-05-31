// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"database/sql"
	"fmt"
	"net/mail"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/facets/contactentity"
	"blackcat.ca/gmeow/internal/facets/mailmessage"
)

type mailParticipantProjectionState struct {
	manifest *contracts.Manifest
	contacts []string
	current  bool
}

func mailParticipantProjectionStateTx(
	ctx context.Context,
	transaction pgx.Tx,
	manifest contracts.Manifest,
) (mailParticipantProjectionState, error) {
	state := mailParticipantProjectionState{
		manifest: &manifest,
		current:  manifestHasFacetKind(manifest, contracts.MailMessageFacetKind),
	}

	previous, err := objectHadMailParticipantRowsTx(
		ctx,
		transaction,
		manifest.ObjectDigest,
	)
	if err != nil {
		return mailParticipantProjectionState{}, err
	}

	if !state.current && !previous {
		return state, nil
	}

	state.contacts, err = contactIDsForMailParticipantsTx(
		ctx,
		transaction,
		manifest.ObjectDigest,
	)
	if err != nil {
		return mailParticipantProjectionState{}, err
	}

	return state, nil
}

func insertMailParticipantProjectionRows(
	ctx context.Context,
	transaction pgx.Tx,
	state *mailParticipantProjectionState,
) error {
	if !state.current {
		return nil
	}

	err := insertMailParticipantRows(ctx, transaction, *state.manifest)
	if err != nil {
		return err
	}

	contacts, err := contactIDsForMailParticipantsTx(
		ctx,
		transaction,
		state.manifest.ObjectDigest,
	)
	if err != nil {
		return err
	}

	state.contacts = append(state.contacts, contacts...)

	return nil
}

func refreshMailParticipantContactRollupsTx(
	ctx context.Context,
	transaction pgx.Tx,
	state mailParticipantProjectionState,
) error {
	contacts := uniqueNonEmptyStrings(state.contacts)
	if len(contacts) == 0 {
		return nil
	}

	return refreshContactRollupsForContactsTx(ctx, transaction, contacts)
}

func insertMailParticipantRows(
	ctx context.Context,
	transaction pgx.Tx,
	manifest contracts.Manifest,
) error {
	metadata, ok := mailMessageFacetMetadata(manifest)
	if !ok {
		return nil
	}

	messageID := strings.TrimSpace(stringFromAny(metadata["rfc_message_id"]))
	messageDate := strings.TrimSpace(stringFromAny(metadata["date"]))
	messageTime := parsedMailTime(messageDate)

	for _, participant := range mailmessage.ParticipantsFromMetadata(metadata) {
		token := contactentity.NormalizeIdentity(participant.Address)
		if token == "" {
			continue
		}

		_, err := transaction.Exec(
			ctx,
			`INSERT INTO query_mail_participants(
			   message_digest, message_id, message_date, message_time, role, ordinal,
			   token_hash, token, display_name, raw_value
			 ) VALUES($1,$2,$3,$4,$5,$6,$7,$8,$9,$10)
			 ON CONFLICT(message_digest, role, ordinal, token_hash) DO UPDATE SET
			   message_id = excluded.message_id,
			   message_date = excluded.message_date,
			   message_time = excluded.message_time,
			   token = excluded.token,
			   display_name = excluded.display_name,
			   raw_value = excluded.raw_value,
			   projected_at = now()`,
			manifest.ObjectDigest,
			messageID,
			messageDate,
			messageTime,
			participant.Role,
			participant.Ordinal,
			hashString(token),
			token,
			participant.DisplayName,
			participant.RawValue,
		)
		if err != nil {
			return fmt.Errorf("insert mail participant projection: %w", err)
		}
	}

	return nil
}

func parsedMailTime(value string) *time.Time {
	value = strings.TrimSpace(value)
	if value == "" {
		return nil
	}

	parsed, err := mail.ParseDate(value)
	if err != nil {
		parsed, err = time.Parse(time.RFC3339, value)
	}

	if err != nil {
		return nil
	}

	parsed = parsed.UTC()

	return &parsed
}

func contactIDsForMailParticipantsTx(
	ctx context.Context,
	tx pgx.Tx,
	digest contracts.ObjectDigest,
) ([]string, error) {
	rows, err := tx.Query(
		ctx,
		`SELECT DISTINCT b.contact_id
		   FROM query_mail_participants p
		   JOIN query_contact_identity_bindings b
		     ON b.token_hash = p.token_hash AND b.token = p.token
		  WHERE p.message_digest = $1
		  ORDER BY b.contact_id`,
		digest,
	)
	if err != nil {
		return nil, fmt.Errorf("query mail participant contacts: %w", err)
	}
	defer rows.Close()

	contacts := []string{}

	for rows.Next() {
		var contactID string

		err = rows.Scan(&contactID)
		if err != nil {
			return nil, fmt.Errorf("scan mail participant contact: %w", err)
		}

		contacts = append(contacts, contactID)
	}

	err = rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate mail participant contacts: %w", err)
	}

	return contacts, nil
}

func objectHadMailParticipantRowsTx(
	ctx context.Context,
	tx pgx.Tx,
	digest contracts.ObjectDigest,
) (bool, error) {
	var found bool

	err := tx.QueryRow(
		ctx,
		`SELECT EXISTS(
		   SELECT 1 FROM query_mail_participants WHERE message_digest = $1
		 )`,
		digest,
	).Scan(&found)
	if err != nil {
		return false, fmt.Errorf("query existing mail participant projection: %w", err)
	}

	return found, nil
}

func (index *Index) ContactMessages(
	ctx context.Context,
	request contracts.ContactMessageRequest,
) (contracts.ContactMessageResponse, error) {
	contactID := strings.TrimSpace(request.ContactID)
	limit := normalizedLimit(request.Limit)
	offset := max(request.Offset, 0)

	if contactID == "" {
		return contracts.ContactMessageResponse{
			SchemaVersion: contracts.SchemaVersionPhase00,
			Results:       []contracts.ContactMessageResult{},
			Limit:         limit,
			Offset:        offset,
		}, nil
	}

	args, whereSQL := contactMessageFilterArgs(request, contactID)

	total, err := countContactMessages(ctx, index, args, whereSQL)
	if err != nil {
		return contracts.ContactMessageResponse{}, err
	}

	results, err := queryContactMessages(ctx, index, args, whereSQL, limit, offset)
	if err != nil {
		return contracts.ContactMessageResponse{}, err
	}

	return contracts.ContactMessageResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Results:       results,
		Total:         total,
		Limit:         limit,
		Offset:        offset,
	}, nil
}

func contactMessageFilterArgs(
	request contracts.ContactMessageRequest,
	contactID string,
) ([]any, string) {
	args := []any{contactID}
	where := []string{"true"}

	if role := strings.TrimSpace(request.Role); role != "" {
		args = append(args, role)
		where = append(where, fmt.Sprintf("p.role = $%d", len(args)))
	}

	return args, strings.Join(where, " AND ")
}

func countContactMessages(
	ctx context.Context,
	index *Index,
	args []any,
	whereSQL string,
) (int, error) {
	total := 0

	err := index.pool.QueryRow(
		ctx,
		`WITH identities AS (
		   SELECT DISTINCT token_hash, token
		     FROM query_contact_identity_bindings b
		    WHERE b.contact_id = $1
		 )
		 SELECT count(*)::integer
		   FROM query_mail_participants p
		   JOIN identities i ON i.token_hash = p.token_hash AND i.token = p.token
		  WHERE `+whereSQL,
		args...,
	).Scan(&total)
	if err != nil {
		return 0, fmt.Errorf("count contact messages: %w", err)
	}

	return total, nil
}

func queryContactMessages(
	ctx context.Context,
	index *Index,
	args []any,
	whereSQL string,
	limit int,
	offset int,
) ([]contracts.ContactMessageResult, error) {
	args = append(append([]any{}, args...), limit, offset)

	rows, err := index.pool.Query(ctx, fmt.Sprintf(
		`WITH identities AS (
		   SELECT DISTINCT token_hash, token
		     FROM query_contact_identity_bindings b
		    WHERE b.contact_id = $1
		 )
		 SELECT p.message_digest, p.message_id, p.message_date, p.message_time,
		        p.role, p.token, p.display_name, p.raw_value
		   FROM query_mail_participants p
		   JOIN identities i ON i.token_hash = p.token_hash AND i.token = p.token
		  WHERE %s
		  ORDER BY p.message_time DESC NULLS LAST,
		           p.message_digest, p.role, p.ordinal
		  LIMIT $%d OFFSET $%d`,
		whereSQL,
		len(args)-1,
		len(args),
	), args...)
	if err != nil {
		return nil, fmt.Errorf("query contact messages: %w", err)
	}
	defer rows.Close()

	results, err := scanContactMessageRows(rows)
	if err != nil {
		return nil, err
	}

	return results, nil
}

func scanContactMessageRows(rows pgx.Rows) ([]contracts.ContactMessageResult, error) {
	results := []contracts.ContactMessageResult{}

	for rows.Next() {
		result, err := scanContactMessageRow(rows)
		if err != nil {
			return nil, err
		}

		results = append(results, result)
	}

	err := rows.Err()
	if err != nil {
		return nil, fmt.Errorf("iterate contact messages: %w", err)
	}

	return results, nil
}

func scanContactMessageRow(rows pgx.Rows) (contracts.ContactMessageResult, error) {
	var (
		result      contracts.ContactMessageResult
		messageTime sql.NullTime
	)

	err := rows.Scan(
		&result.MessageDigest,
		&result.MessageID,
		&result.MessageDate,
		&messageTime,
		&result.Role,
		&result.Token,
		&result.DisplayName,
		&result.RawValue,
	)
	if err != nil {
		return contracts.ContactMessageResult{}, fmt.Errorf("scan contact message: %w", err)
	}

	if messageTime.Valid {
		result.MessageTime = messageTime.Time
	}

	return result, nil
}
