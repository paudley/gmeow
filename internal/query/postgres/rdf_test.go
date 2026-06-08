// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package postgres

import "testing"

// The Turtle/RDF-star parser tests moved with the parser to internal/rdfbundle.
// rdfBundleText is the projection-side UTF-8 normalization wrapper and stays here.
func TestRDFBundleTextNormalizesInvalidUTF8(t *testing.T) {
	content := rdfBundleText([]byte{
		'<', 's', '>', ' ', '<', 'p', '>', ' ', '"', 'b', 'a', 'd', ' ', 0x8d,
		' ', 'b', 'y', 't', 'e', '"', ' ', '.', '\n',
	})
	if content != `<s> <p> "bad ? byte" .`+"\n" {
		t.Fatalf("invalid UTF-8 was not normalized: %q", content)
	}
}
