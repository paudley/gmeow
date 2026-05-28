// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"strings"
	"testing"
	"unicode/utf8"

	"blackcat.ca/gmeow/internal/contracts"
)

func TestReadOnlyCypherValidation(t *testing.T) {
	accepted := []string{
		"MATCH (n) RETURN n",
		"match (n)-[r]->(m) return n, r, m limit 10",
	}
	for _, query := range accepted {
		if !readOnlyCypher(query) {
			t.Fatalf("expected query to be accepted: %s", query)
		}
	}
	rejected := []string{
		"CREATE (n)",
		"MATCH (n) SET n.name = 'bad' RETURN n",
		"MATCH (n) DELETE n",
		"MATCH (n) RETURN n CALL db.labels()",
		"MATCH (n)",
	}
	for _, query := range rejected {
		if readOnlyCypher(query) {
			t.Fatalf("expected query to be rejected: %s", query)
		}
	}
}

func TestSearchTextIsBoundedForPostgresTSVector(t *testing.T) {
	large := strings.Repeat("searchable ", maxSearchTextBytes/len("searchable ")+100)
	text := searchText(
		contracts.Manifest{
			ObjectID:     "large",
			MediaType:    "message/rfc822",
			ObjectDigest: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		},
		[]contracts.Annotation{{
			Kind:         "analysis",
			AnalyzerName: "large",
			AnalyzerVer:  "1",
			Data:         map[string]any{"body": large + "é"},
		}},
	)
	if len(text) > maxSearchTextBytes {
		t.Fatalf("search text exceeded bound: %d", len(text))
	}
	if !utf8.ValidString(text) {
		t.Fatal("search text truncation produced invalid UTF-8")
	}
}

func TestValidateAgeColumns(t *testing.T) {
	if err := validateAgeColumns("value agtype, count bigint"); err != nil {
		t.Fatal(err)
	}
	for _, columns := range []string{
		"value agtype); DROP TABLE query_objects; --",
		"value",
		"value jsonb",
	} {
		if err := validateAgeColumns(columns); err == nil {
			t.Fatalf("expected AGE columns to be rejected: %s", columns)
		}
	}
}

func TestRelationshipFilterActive(t *testing.T) {
	if relationshipFilterActive(contracts.RelationshipFilter{}) {
		t.Fatal("empty relationship filter should be inactive")
	}
	if !relationshipFilterActive(contracts.RelationshipFilter{Roles: []string{"body"}}) {
		t.Fatal("role relationship filter should be active")
	}
}
