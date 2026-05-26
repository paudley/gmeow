// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"context"
	"errors"
	"fmt"
	"strings"
)

func (index *Index) AgeStatus(ctx context.Context) AgeStatus {
	conn, err := acquireAgeConn(ctx, index.pool)
	if err != nil {
		return AgeStatus{Graph: "gmeow_graph", Error: err.Error()}
	}
	defer conn.Release()

	var status AgeStatus

	status.Graph = "gmeow_graph"

	err = conn.QueryRow(
		ctx,
		"SELECT graphid, name FROM ag_graph WHERE name = 'gmeow_graph'",
	).Scan(&status.GraphID, &status.Graph)
	if err != nil {
		status.Error = err.Error()

		return status
	}

	var nodes string
	if err := conn.QueryRow(
		ctx,
		"SELECT * FROM cypher('gmeow_graph', $$MATCH (n) RETURN count(n)$$) AS (nodes agtype)",
	).Scan(&nodes); err != nil {
		status.Error = err.Error()

		return status
	}

	status.Available = true
	status.Nodes = nodes

	return status
}

func (index *Index) AgeCypher(
	ctx context.Context,
	cypher string,
	columns string,
	limit int,
) ([]map[string]string, error) {
	if !readOnlyCypher(cypher) {
		return nil, errors.New("only read-only MATCH/RETURN Cypher queries are allowed")
	}

	if strings.TrimSpace(columns) == "" {
		columns = "value agtype"
	}

	if err := validateAgeColumns(columns); err != nil {
		return nil, err
	}

	cypher = strings.TrimSpace(strings.TrimSuffix(cypher, ";"))
	if !strings.Contains(" "+strings.ToLower(cypher)+" ", " limit ") {
		cypher = fmt.Sprintf("%s LIMIT %d", cypher, normalizedLimit(limit))
	}

	conn, err := acquireAgeConn(ctx, index.pool)
	if err != nil {
		return nil, err
	}
	defer conn.Release()

	rows, err := conn.Query(ctx, ageSQL(cypher, columns))
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	fieldDescriptions := rows.FieldDescriptions()
	results := []map[string]string{}

	for rows.Next() {
		values, err := rows.Values()
		if err != nil {
			return nil, err
		}

		row := map[string]string{}
		for index, value := range values {
			row[string(fieldDescriptions[index].Name)] = fmt.Sprint(value)
		}

		results = append(results, row)
	}

	return results, rows.Err()
}

func (index *Index) clearAgeGraph(ctx context.Context) error {
	conn, err := acquireAgeConn(ctx, index.pool)
	if err != nil {
		return err
	}
	defer conn.Release()

	if _, err := conn.Exec(
		ctx,
		"SELECT * FROM cypher('gmeow_graph', $$MATCH ()-[r]->() DELETE r$$) AS (value agtype)",
	); err != nil {
		return fmt.Errorf("clear AGE graph edges: %w", err)
	}

	if _, err := conn.Exec(
		ctx,
		"SELECT * FROM cypher('gmeow_graph', $$MATCH (n) DELETE n$$) AS (value agtype)",
	); err != nil {
		return fmt.Errorf("clear AGE graph nodes: %w", err)
	}

	return nil
}

func ageSQL(cypher, columns string) string {
	return fmt.Sprintf(
		"SELECT * FROM cypher('gmeow_graph', %s) AS (%s)",
		dollarQuote(cypher),
		columns,
	)
}

func dollarQuote(value string) string {
	tag := "gmeow_age"
	for strings.Contains(value, "$"+tag+"$") {
		tag = "_" + tag
	}

	return "$" + tag + "$" + value + "$" + tag + "$"
}

func ageStringLiteral(value string) string {
	return "'" + strings.ReplaceAll(value, "'", "\\'") + "'"
}

func readOnlyCypher(query string) bool {
	lowered := strings.ToLower(strings.TrimSpace(query))
	if !strings.HasPrefix(lowered, "match ") {
		return false
	}

	padded := " " + lowered + " "
	for _, word := range []string{
		" create ", " merge ", " delete ", " detach ", " set ", " remove ", " drop ", " call ",
	} {
		if strings.Contains(padded, word) {
			return false
		}
	}

	return strings.Contains(padded, " return ")
}

func validateAgeColumns(columns string) error {
	for _, char := range columns {
		if (char >= 'a' && char <= 'z') ||
			(char >= 'A' && char <= 'Z') ||
			(char >= '0' && char <= '9') ||
			char == '_' ||
			char == ',' ||
			char == ' ' {
			continue
		}

		return fmt.Errorf("invalid AGE column declaration %q", columns)
	}

	for _, part := range strings.Split(columns, ",") {
		fields := strings.Fields(part)
		if len(fields) != 2 {
			return fmt.Errorf("invalid AGE column declaration %q", columns)
		}

		switch fields[1] {
		case "agtype", "text", "bigint", "int", "float8", "boolean":
		default:
			return fmt.Errorf("unsupported AGE column type %q", fields[1])
		}
	}

	return nil
}
