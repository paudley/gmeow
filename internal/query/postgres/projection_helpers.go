// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"

	"blackcat.ca/gmeow/internal/contracts"
)

type summaryRow struct {
	metadata map[string]any
	kind     string
	text     string
}

func summaryRowsFrom(
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) []summaryRow {
	rows := []summaryRow{}

	for _, annotation := range annotations {
		if annotation.Kind != "analysis" {
			continue
		}

		if summary := stringFromAny(annotation.Data["summary"]); summary != "" {
			rows = append(rows, summaryRow{
				kind:     firstNonEmpty(annotation.AnalyzerName, "analysis"),
				text:     summary,
				metadata: map[string]any{"analyzer_version": annotation.AnalyzerVer},
			})
		}
	}

	if summary := stringFromAny(manifest.Analysis["summary"]); summary != "" {
		rows = append(rows, summaryRow{kind: "manifest", text: summary})
	}

	return rows
}

func searchText(
	manifest contracts.Manifest,
	annotations []contracts.Annotation,
) string {
	parts := []string{
		manifest.ObjectID,
		manifest.MediaType,
		strings.Join(manifest.ContentRoles, " "),
		strings.Join(manifest.Keywords, " "),
	}
	for _, title := range manifest.Titles {
		parts = append(parts, title.Value)
	}

	for _, facet := range manifest.Facets {
		parts = append(parts, facet.Kind, facet.Name)
		if len(facet.Metadata) > 0 {
			encoded, err := marshalPostgresJSON(facet.Metadata)
			if err == nil {
				parts = append(parts, string(encoded))
			}
		}
	}

	for _, provenance := range manifest.Provenance {
		parts = append(
			parts,
			provenance.SourceKind,
			provenance.SourceName,
			provenance.ExternalID,
		)
	}

	encoded, err := marshalPostgresJSON(annotations)
	if err == nil {
		parts = append(parts, string(encoded))
	}

	return truncateSearchText(stripPostgresNUL(strings.Join(parts, "\n")))
}

func marshalPostgresJSON(value any) ([]byte, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("encode postgres json payload: %w", err)
	}

	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()

	var decoded any

	err = decoder.Decode(&decoded)
	if err != nil {
		return nil, fmt.Errorf("decode postgres json payload: %w", err)
	}

	sanitized, err := json.Marshal(sanitizePostgresJSON(decoded))
	if err != nil {
		return nil, fmt.Errorf("encode sanitized postgres json payload: %w", err)
	}

	return sanitized, nil
}

func sanitizePostgresJSON(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		result := make(map[string]any, len(typed))
		for key, item := range typed {
			result[stripPostgresNUL(key)] = sanitizePostgresJSON(item)
		}

		return result
	case []any:
		result := make([]any, 0, len(typed))
		for _, item := range typed {
			result = append(result, sanitizePostgresJSON(item))
		}

		return result
	case string:
		return stripPostgresNUL(typed)
	default:
		return value
	}
}

func stripPostgresNUL(value string) string {
	return strings.ReplaceAll(value, "\x00", "")
}

func truncateSearchText(text string) string {
	if len(text) <= maxSearchTextBytes {
		return text
	}

	limit := 0

	for offset := range text {
		if offset > maxSearchTextBytes {
			break
		}

		limit = offset
	}

	if limit == 0 {
		return ""
	}

	return text[:limit]
}
