// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"encoding/json"
	"io"
	"math"
	"strconv"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
	"blackcat.ca/gmeow/internal/query"
)

type embeddingObjectReader interface {
	Open(context.Context, contracts.ObjectDigest) (io.ReadCloser, error)
}

type embeddingRow struct {
	vector     *string
	model      string
	digest     contracts.ObjectDigest
	dimensions int
}

func embeddingRowsFrom(
	ctx context.Context,
	source query.ProjectionSource,
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) []embeddingRow {
	rows := []embeddingRow{}

	for _, item := range manifest.Embeddings {
		vector := embeddingVectorFromSource(ctx, source, item.ObjectDigest)
		rows = append(rows, embeddingRow{
			model:      item.Model,
			digest:     item.ObjectDigest,
			dimensions: item.Dimensions,
			vector:     vector,
		})
	}

	for _, annotation := range annotations {
		values, ok := annotation.Data["embeddings"].([]any)
		if !ok {
			continue
		}

		for _, value := range values {
			item, ok := value.(map[string]any)
			if !ok {
				continue
			}

			vector := vectorAnyLiteral(item["vector"])
			rows = append(rows, embeddingRow{
				model:      stringFromAny(item["model"]),
				digest:     contracts.ObjectDigest(stringFromAny(item["object_digest"])),
				dimensions: intFromAny(item["dimensions"]),
				vector:     vector,
			})
		}
	}

	return rows
}

func embeddingVectorFromSource(
	ctx context.Context,
	source query.ProjectionSource,
	digest contracts.ObjectDigest,
) *string {
	reader, ok := source.(embeddingObjectReader)
	if !ok || digest == "" {
		return nil
	}

	opened, err := reader.Open(ctx, digest)
	if err != nil {
		return nil
	}
	defer opened.Close()

	content, err := io.ReadAll(opened)
	if err != nil {
		return nil
	}

	return vectorJSONLiteral(content)
}

func vectorJSONLiteral(content []byte) *string {
	var value any
	err := json.Unmarshal(content, &value)
	if err != nil {
		return nil
	}

	return vectorValueLiteral(value)
}

func vectorValueLiteral(value any) *string {
	switch typed := value.(type) {
	case []any:
		return vectorAnyLiteral(typed)
	case map[string]any:
		for _, key := range []string{"vector", "embedding"} {
			if vector := vectorValueLiteral(typed[key]); vector != nil {
				return vector
			}
		}

		values, ok := typed["embeddings"].([]any)
		if ok && len(values) > 0 {
			return vectorValueLiteral(values[0])
		}
	}

	return nil
}

func vectorLiteral(values []float32) string {
	parts := make([]string, 0, len(values))
	for _, value := range values {
		parts = append(parts, strconv.FormatFloat(float64(value), 'f', -1, 32))
	}

	return "[" + strings.Join(parts, ",") + "]"
}

func vectorAnyLiteral(value any) *string {
	items, ok := value.([]any)
	if !ok || len(items) == 0 {
		return nil
	}

	values := make([]float32, 0, len(items))
	for _, item := range items {
		switch typed := item.(type) {
		case float64:
			if math.IsNaN(typed) || math.IsInf(typed, 0) {
				return nil
			}

			values = append(values, float32(typed))
		case float32:
			values = append(values, typed)
		}
	}

	literal := vectorLiteral(values)

	return &literal
}
