// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"fmt"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

// relatedObjectsSQL ranks candidate objects by their best (minimum) chunk-pair
// distance to the seed object's stored embeddings. DISTINCT ON collapses each
// candidate's many chunks to its single closest match before the outer ORDER BY
// ranks candidates. The seed is excluded and candidates are restricted to the
// requested facet.
const relatedObjectsSQL = `
WITH seed AS (
    SELECT embedding, model, dimensions
      FROM query_object_embeddings
     WHERE object_digest = $1 AND embedding IS NOT NULL
)
SELECT best.object_digest, best.model, best.embedding_id, best.kind,
       best.source_digest, best.text_preview, best.distance
  FROM (
        SELECT DISTINCT ON (e.object_digest)
               e.object_digest, e.model, e.embedding_id, e.kind,
               e.source_digest, e.text_preview,
               e.embedding <=> seed.embedding AS distance
          FROM query_object_embeddings e
          JOIN seed ON e.model = seed.model AND e.dimensions = seed.dimensions
         WHERE e.object_digest <> $1
           AND e.embedding IS NOT NULL
           AND EXISTS (
                 SELECT 1 FROM query_object_facets f
                  WHERE f.object_digest = e.object_digest AND f.kind = $2
               )
         ORDER BY e.object_digest, distance
       ) best
 ORDER BY best.distance
 LIMIT $3`

// RelatedObjects returns the objects most similar to a seed object, scored
// against the seed's own already-stored embeddings — no query-text embedding is
// needed. The seed itself is excluded and results are restricted to the
// requested facet (default mail_message) so it surfaces "more like this
// message".
func (index *Index) RelatedObjects(
	ctx context.Context,
	request contracts.RelatedObjectsRequest,
) (contracts.RelatedObjectsResponse, error) {
	response := contracts.RelatedObjectsResponse{
		SchemaVersion: contracts.SchemaVersionPhase00,
		Seed:          request.Digest,
	}
	if strings.TrimSpace(string(request.Digest)) == "" {
		return response, nil
	}

	facet := strings.TrimSpace(request.Facet)
	if facet == "" {
		facet = "mail_message"
	}

	rows, err := index.pool.Query(
		ctx,
		relatedObjectsSQL,
		string(request.Digest),
		facet,
		normalizedLimit(request.Limit),
	)
	if err != nil {
		return response, fmt.Errorf("related objects: %w", err)
	}
	defer rows.Close()

	results := []contracts.VectorSearchResult{}

	for rows.Next() {
		var result contracts.VectorSearchResult

		err := rows.Scan(
			&result.ObjectDigest,
			&result.Model,
			&result.EmbeddingID,
			&result.Kind,
			&result.SourceDigest,
			&result.TextPreview,
			&result.Distance,
		)
		if err != nil {
			return response, err
		}

		results = append(results, result)
	}

	response.Results = results

	return response, rows.Err()
}
