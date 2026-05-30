// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"fmt"
)

// CountRow is one labelled count in an object breakdown (a facet kind, media
// type, identity strategy, or analyzer name).
type CountRow struct {
	Label string `json:"label"`
	Count int64  `json:"count"`
}

// SourceRow is the count of distinct objects provenanced to one source.
type SourceRow struct {
	SourceKind string `json:"source_kind"`
	SourceName string `json:"source_name"`
	Objects    int64  `json:"objects"`
}

// ObjectBreakdown is a detailed aggregate view of the projected object corpus.
type ObjectBreakdown struct {
	TotalObjects        int64       `json:"total_objects"`
	TotalSizeBytes      int64       `json:"total_size_bytes"`
	CompoundObjects     int64       `json:"compound_objects"`
	SimpleObjects       int64       `json:"simple_objects"`
	ObjectsWithAnalysis int64       `json:"objects_with_analysis"`
	ByFacet             []CountRow  `json:"by_facet"`
	BySource            []SourceRow `json:"by_source"`
	ByMediaType         []CountRow  `json:"by_media_type"`
	ByIdentityStrategy  []CountRow  `json:"by_identity_strategy"`
	ByAnalyzer          []CountRow  `json:"by_analyzer"`
}

const compoundIdentityStrategy = "compound_stable_id"

// ObjectBreakdown aggregates the projected corpus across facets, sources, media
// types, identity strategies, and analysis coverage. It reads only the query
// projection (no FILESTORE access), so it reflects what has been projected so
// far — run `query rebuild` or let projection catch up for a complete picture.
func (index *Index) ObjectBreakdown(ctx context.Context) (ObjectBreakdown, error) {
	breakdown := ObjectBreakdown{}

	if err := index.pool.QueryRow(ctx, `
		SELECT count(*),
		       coalesce(sum(size_bytes), 0),
		       count(*) FILTER (WHERE identity_strategy = $1),
		       count(*) FILTER (WHERE identity_strategy <> $1)
		  FROM query_objects`, compoundIdentityStrategy,
	).Scan(
		&breakdown.TotalObjects,
		&breakdown.TotalSizeBytes,
		&breakdown.CompoundObjects,
		&breakdown.SimpleObjects,
	); err != nil {
		return ObjectBreakdown{}, fmt.Errorf("query object totals: %w", err)
	}

	if err := index.pool.QueryRow(ctx, `
		SELECT count(DISTINCT object_digest) FROM query_object_analysis`,
	).Scan(&breakdown.ObjectsWithAnalysis); err != nil {
		return ObjectBreakdown{}, fmt.Errorf("query analysis coverage: %w", err)
	}

	var err error
	if breakdown.ByFacet, err = index.countRows(ctx, `
		SELECT kind, count(*) FROM query_object_facets
		 GROUP BY kind ORDER BY count(*) DESC, kind`); err != nil {
		return ObjectBreakdown{}, fmt.Errorf("query facet breakdown: %w", err)
	}

	if breakdown.ByMediaType, err = index.countRows(ctx, `
		SELECT media_type, count(*) FROM query_objects
		 GROUP BY media_type ORDER BY count(*) DESC, media_type`); err != nil {
		return ObjectBreakdown{}, fmt.Errorf("query media-type breakdown: %w", err)
	}

	if breakdown.ByIdentityStrategy, err = index.countRows(ctx, `
		SELECT identity_strategy, count(*) FROM query_objects
		 GROUP BY identity_strategy ORDER BY count(*) DESC, identity_strategy`); err != nil {
		return ObjectBreakdown{}, fmt.Errorf("query identity-strategy breakdown: %w", err)
	}

	if breakdown.ByAnalyzer, err = index.countRows(ctx, `
		SELECT analyzer_name, count(DISTINCT object_digest) FROM query_object_analysis
		 GROUP BY analyzer_name ORDER BY count(DISTINCT object_digest) DESC, analyzer_name`); err != nil {
		return ObjectBreakdown{}, fmt.Errorf("query analyzer breakdown: %w", err)
	}

	if breakdown.BySource, err = index.sourceRows(ctx); err != nil {
		return ObjectBreakdown{}, err
	}

	return breakdown, nil
}

func (index *Index) countRows(ctx context.Context, sql string) ([]CountRow, error) {
	rows, err := index.pool.Query(ctx, sql)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	out := []CountRow{}
	for rows.Next() {
		var row CountRow
		if err := rows.Scan(&row.Label, &row.Count); err != nil {
			return nil, err
		}
		out = append(out, row)
	}

	return out, rows.Err()
}

func (index *Index) sourceRows(ctx context.Context) ([]SourceRow, error) {
	rows, err := index.pool.Query(ctx, `
		SELECT source_kind, source_name, count(DISTINCT object_digest)
		  FROM query_object_provenance
		 GROUP BY source_kind, source_name
		 ORDER BY count(DISTINCT object_digest) DESC, source_kind, source_name`)
	if err != nil {
		return nil, fmt.Errorf("query source breakdown: %w", err)
	}
	defer rows.Close()

	out := []SourceRow{}
	for rows.Next() {
		var row SourceRow
		if err := rows.Scan(&row.SourceKind, &row.SourceName, &row.Objects); err != nil {
			return nil, err
		}
		out = append(out, row)
	}

	return out, rows.Err()
}
