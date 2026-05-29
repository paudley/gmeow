// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"fmt"
	"strings"

	"github.com/jackc/pgx/v5"
	"github.com/zeebo/blake3"
)

const bearerTokenTable = "public.jmap_bearer_tokens"

// bearerTokenBytes is the count of cryptographically random bytes per token.
// Hex encoding doubles the length, so 64 bytes yields a 128-character token.
const bearerTokenBytes = 64

// CreateBearerToken mints a cryptographically random 128-character bearer token
// for the given client identifier, persisting only its BLAKE3 hash. The plaintext
// token is returned once and is never stored, so it cannot be recovered later.
func (index *Index) CreateBearerToken(
	ctx context.Context,
	clientID string,
) (string, error) {
	trimmed := strings.TrimSpace(clientID)
	if trimmed == "" {
		return "", errors.New("client id is required")
	}

	raw := make([]byte, bearerTokenBytes)
	if _, err := rand.Read(raw); err != nil {
		return "", fmt.Errorf("generate bearer token: %w", err)
	}
	token := hex.EncodeToString(raw)
	hash := blake3.Sum256([]byte(token))

	_, err := index.pool.Exec(ctx, `
		INSERT INTO `+bearerTokenTable+` (token_hash, client_id)
		VALUES ($1, $2)`,
		hash[:],
		trimmed,
	)
	if err != nil {
		return "", err
	}

	return token, nil
}

// ValidateBearerToken reports whether the presented token matches a stored token
// and returns the associated client identifier. An unknown or empty token is not
// an error: it returns ok=false.
func (index *Index) ValidateBearerToken(
	ctx context.Context,
	token string,
) (string, bool, error) {
	trimmed := strings.TrimSpace(token)
	if trimmed == "" {
		return "", false, nil
	}

	hash := blake3.Sum256([]byte(trimmed))

	var clientID string
	err := index.pool.QueryRow(ctx, `
		SELECT client_id
		FROM `+bearerTokenTable+`
		WHERE token_hash = $1`,
		hash[:],
	).Scan(&clientID)
	if err != nil {
		if errors.Is(err, pgx.ErrNoRows) {
			return "", false, nil
		}

		return "", false, err
	}

	return clientID, true, nil
}
