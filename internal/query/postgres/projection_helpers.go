// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"encoding/json"
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
			encoded, err := json.Marshal(facet.Metadata)
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

	encoded, err := json.Marshal(annotations)
	if err == nil {
		parts = append(parts, string(encoded))
	}

	return truncateSearchText(strings.Join(parts, "\n"))
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
