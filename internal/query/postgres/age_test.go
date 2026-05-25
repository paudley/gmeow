// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"os"
	"strings"
	"testing"

	"blackat.ca/gmeow/internal/contracts"
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

func TestMigrationRequiresExistingExtensionsWithoutCreatingThem(t *testing.T) {
	content, err := os.ReadFile("../../../migrations/query/00001_query_projection.sql")
	if err != nil {
		t.Fatal(err)
	}
	sql := string(content)
	if strings.Contains(strings.ToLower(sql), "create extension") {
		t.Fatal("QUERY migration must not create PostgreSQL extensions")
	}
	vectorCheck := strings.Index(sql, "extname = 'vector'")
	ageCheck := strings.Index(sql, "extname = 'age'")
	vectorColumn := strings.Index(sql, "embedding vector")
	createGraph := strings.Index(sql, "create_graph('gmeow_graph')")
	if vectorCheck < 0 || ageCheck < 0 {
		t.Fatalf("migration must validate vector and age extensions:\n%s", sql)
	}
	if vectorColumn < 0 || vectorCheck > vectorColumn {
		t.Fatal("vector extension validation must appear before embedding vector column")
	}
	if createGraph < 0 || ageCheck > createGraph {
		t.Fatal("age extension validation must appear before gmeow_graph bootstrap")
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
