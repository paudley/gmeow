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

	return stripPostgresNULJSONEscapes(encoded), nil
}

func stripPostgresNULJSONEscapes(encoded []byte) []byte {
	if !bytes.Contains(encoded, []byte(`\u0000`)) {
		return encoded
	}

	result := make([]byte, 0, len(encoded))
	for offset := 0; offset < len(encoded); offset++ {
		if isPostgresNULJSONEscape(encoded, offset) {
			offset += len(`\u0000`) - 1

			continue
		}

		result = append(result, encoded[offset])
	}

	return result
}

func isPostgresNULJSONEscape(encoded []byte, offset int) bool {
	if offset+len(`\u0000`) > len(encoded) ||
		!bytes.Equal(encoded[offset:offset+len(`\u0000`)], []byte(`\u0000`)) {
		return false
	}

	backslashes := 0
	for cursor := offset - 1; cursor >= 0 && encoded[cursor] == '\\'; cursor-- {
		backslashes++
	}

	return backslashes%2 == 0
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
