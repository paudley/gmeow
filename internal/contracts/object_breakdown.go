// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

// BreakdownCount is one labelled count in an object breakdown (a facet kind,
// media type, identity strategy, or analyzer name).
type BreakdownCount struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

// BreakdownSource is the count of distinct objects provenanced to one source.
type BreakdownSource struct {
	SourceKind string `json:"source_kind"`
	SourceName string `json:"source_name"`
	Objects    int64  `json:"objects"`
}

// ObjectBreakdown is a detailed aggregate view of the projected object corpus.
type ObjectBreakdown struct {
	TotalObjects        int64             `json:"total_objects"`
	TotalSizeBytes      int64             `json:"total_size_bytes"`
	CompoundObjects     int64             `json:"compound_objects"`
	SimpleObjects       int64             `json:"simple_objects"`
	ObjectsWithAnalysis int64             `json:"objects_with_analysis"`
	ByFacet             []BreakdownCount  `json:"by_facet"`
	BySource            []BreakdownSource `json:"by_source"`
	ByMediaType         []BreakdownCount  `json:"by_media_type"`
	ByIdentityStrategy  []BreakdownCount  `json:"by_identity_strategy"`
	ByAnalyzer          []BreakdownCount  `json:"by_analyzer"`
}
