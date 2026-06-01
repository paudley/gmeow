// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"strings"
	"testing"
)

func TestTruncateProjectionTablesClearsContactAnalysisState(t *testing.T) {
	query := truncateProjectionTablesSQL()

	for _, table := range []string{
		"query_contact_aliases",
		"query_contact_analysis",
		"query_contact_embeddings",
	} {
		if !strings.Contains(query, table) {
			t.Fatalf("truncate projection SQL missing %s: %s", table, query)
		}
	}
}
