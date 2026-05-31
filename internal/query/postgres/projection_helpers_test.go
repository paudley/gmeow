// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"bytes"
	"testing"
)

func TestMarshalPostgresJSONStripsNULCharacters(t *testing.T) {
	encoded, err := marshalPostgresJSON(map[string]any{
		"subject": "hello\x00world",
		"nested":  []any{"keep", "drop\x00nul"},
	})
	if err != nil {
		t.Fatal(err)
	}

	if bytes.Contains(encoded, []byte(`\u0000`)) || bytes.Contains(encoded, []byte{0}) {
		t.Fatalf("expected postgres-safe json, got %s", encoded)
	}
}
