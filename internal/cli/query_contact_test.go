// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"strings"
	"testing"
	"unicode/utf8"
)

func TestPreviewContactInputPreservesUTF8(t *testing.T) {
	preview := previewContactInput(strings.Repeat("é", 130))
	if !utf8.ValidString(preview) {
		t.Fatalf("preview is not valid UTF-8: %q", preview)
	}
	if len(preview) > 240 {
		t.Fatalf("preview exceeded byte limit: %d", len(preview))
	}
}
