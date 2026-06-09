// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPostgresTextFromAnyNormalizesInvalidUTF8AndNUL(t *testing.T) {
	preview := postgresTextFromAny("bad " + string([]byte{0x8d}) + " \x00 byte")

	if !utf8.ValidString(preview) {
		t.Fatalf("preview is not valid UTF-8: %q", preview)
	}
	if strings.Contains(preview, "\x00") {
		t.Fatalf("preview still contains NUL: %q", preview)
	}
	if preview != "bad ? ? byte" {
		t.Fatalf("unexpected preview normalization: %q", preview)
	}
}
