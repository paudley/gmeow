// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package analysis

import (
	"time"

	"blackcat.ca/gmeow/internal/cache"
	"blackcat.ca/gmeow/internal/contracts"
)

// extractedTextCacheSize bounds the per-process memoization of analysisText.
const extractedTextCacheSize = 8192

type extractedText struct {
	text  string
	bytes int
}

// extractedTextCache memoizes analysisText output so multiple analyzers run on
// the same object (summary, NER, categories, embedding) share one extraction
// instead of each re-reading and re-parsing the body. The key combines the
// content-addressed object digest with the manifest's UpdatedAt: object/part
// content is immutable, and folding in UpdatedAt means any manifest change
// produces a fresh key rather than a stale hit.
var extractedTextCache = cache.NewLRU[string, extractedText](extractedTextCacheSize)

func extractedTextKey(
	digest contracts.ObjectDigest,
	manifest contracts.Manifest,
) string {
	return string(digest) + "\x00" + manifest.UpdatedAt.Format(time.RFC3339Nano)
}
