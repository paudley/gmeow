// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package appsvc

import (
	"context"
	"errors"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

const defaultSimilarMessageLimit = 3

// SimilarMessage is one related-message hit: a compact message-list item plus
// the embedding distance to the seed (smaller is more similar).
type SimilarMessage struct {
	MessageSummaryListItem

	Distance float64 `json:"distance"`
}

type SimilarMessagesResponse struct {
	Seed     string           `json:"seed_digest"`
	Messages []SimilarMessage `json:"messages"`
	Returned int              `json:"returned"`
}

// SimilarMessages surfaces the messages most similar to a given message, ranked
// against that message's own already-stored embeddings — no query-text embedding
// is required. It powers a "more like this message" view.
func (services *Services) SimilarMessages(
	ctx context.Context,
	digest string,
	limit int,
) (SimilarMessagesResponse, error) {
	if strings.TrimSpace(digest) == "" {
		return SimilarMessagesResponse{}, errors.New("similar messages requires a digest")
	}

	if limit <= 0 {
		limit = defaultSimilarMessageLimit
	}

	related, err := services.query.RelatedObjects(ctx, contracts.RelatedObjectsRequest{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Digest:        contracts.ObjectDigest(digest),
		Facet:         "mail_message",
		Limit:         limit,
	})
	if err != nil {
		return SimilarMessagesResponse{}, err
	}

	messages := make([]SimilarMessage, 0, len(related.Results))
	for _, result := range related.Results {
		item := MessageSummaryListItem{Digest: string(result.ObjectDigest)}

		message, msgErr := services.canonicalMessage(ctx, result.ObjectDigest)
		if msgErr == nil {
			item = summaryListItem(message)
		}

		messages = append(messages, SimilarMessage{
			MessageSummaryListItem: item,
			Distance:               result.Distance,
		})
	}

	return SimilarMessagesResponse{
		Seed:     string(related.Seed),
		Messages: messages,
		Returned: len(messages),
	}, nil
}
