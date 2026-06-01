// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import "strings"

func truncateProjectionTablesSQL() string {
	return strings.Join([]string{
		"TRUNCATE",
		"query_projection_state,",
		"query_summaries,",
		"query_source_cursors,",
		"query_mail_participants,",
		"query_contact_embeddings,",
		"query_contact_analysis,",
		"query_contact_rollups,",
		"query_contact_aliases,",
		"query_contact_identity_bindings,",
		"query_contact_facts,",
		"query_rdf_statement_annotations,",
		"query_rdf_statements,",
		"query_rdf_terms,",
		"query_objects",
		"CASCADE",
	}, " ")
}

func deleteProjectionRowsSQL(table string) string {
	queries := map[string]string{
		"query_object_facets":             "DELETE FROM query_object_facets WHERE object_digest = $1",
		"query_object_provenance":         "DELETE FROM query_object_provenance WHERE object_digest = $1",
		"query_object_relationships":      "DELETE FROM query_object_relationships WHERE object_digest = $1",
		"query_object_compound_parts":     "DELETE FROM query_object_compound_parts WHERE object_digest = $1",
		"query_object_analysis":           "DELETE FROM query_object_analysis WHERE object_digest = $1",
		"query_object_graph_edges":        "DELETE FROM query_object_graph_edges WHERE object_digest = $1",
		"query_object_keywords":           "DELETE FROM query_object_keywords WHERE object_digest = $1",
		"query_object_embeddings":         "DELETE FROM query_object_embeddings WHERE object_digest = $1",
		"query_object_overlays":           "DELETE FROM query_object_overlays WHERE object_digest = $1",
		"query_summaries":                 "DELETE FROM query_summaries WHERE object_digest = $1",
		"query_mail_identities":           "DELETE FROM query_mail_identities WHERE object_digest = $1",
		"query_mail_participants":         "DELETE FROM query_mail_participants WHERE message_digest = $1",
		"query_rdf_statement_annotations": "DELETE FROM query_rdf_statement_annotations WHERE source_digest = $1",
		"query_rdf_statements":            "DELETE FROM query_rdf_statements WHERE source_digest = $1",
	}

	query, ok := queries[table]
	if !ok {
		panic("unsupported query projection delete table: " + table)
	}

	return query
}
