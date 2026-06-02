// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contactio

import (
	"errors"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"testing"
	"time"
)

// TestCorpusDryRun is a read-only diagnostic: it runs the existing parsers over
// a real contact corpus and tallies how many files import vs. reject and why.
// It writes nothing and is skipped unless GMEOW_CORPUS_DIR points at a directory.
// Purpose: ground the ingestion-redesign phase work in what the real data does,
// without needing a running FILESTORE.
func TestCorpusDryRun(t *testing.T) {
	root := os.Getenv("GMEOW_CORPUS_DIR")
	if root == "" {
		t.Skip("set GMEOW_CORPUS_DIR to run the corpus dry-run diagnostic")
	}

	formatByExt := map[string]string{
		".vcf":   FormatVCard,
		".csv":   FormatCSV,
		".ttl":   FormatRDF,
		".ged":   FormatGEDCOM,
		".bbdb":  FormatBBDB,
		".abcdp": FormatAppleAddressBook,
		".abcdg": FormatAppleAddressBookGroup,
	}

	type stat struct {
		files    int
		ok       int
		failed   int
		skipped  int
		contacts int
		rejected int
	}
	byFmt := map[string]*stat{}
	distinctByFmt := map[string]map[string]struct{}{}
	reasons := map[string]int{}
	var failSamples []string

	walkErr := filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return nil //nolint:nilerr // diagnostic: skip unreadable entries
		}
		format, known := formatByExt[strings.ToLower(filepath.Ext(path))]
		if !known {
			return nil
		}
		s := byFmt[format]
		if s == nil {
			s = &stat{}
			byFmt[format] = s
		}
		s.files++

		content, readErr := os.ReadFile(path) //nolint:gosec // diagnostic over a user-provided dir
		if readErr != nil {
			s.failed++
			reasons["read error"]++
			return nil
		}
		_, result, importErr := BuildImportObject(format, "corpus-dryrun", path, content, time.Time{})
		if errors.Is(importErr, ErrSkipNonContactDomain) {
			s.skipped++
			reasons["skipped: not contact-domain"]++
			return nil
		}
		if importErr != nil {
			s.failed++
			reasons[classifyDryRunError(importErr.Error())]++
			if len(failSamples) < 40 {
				failSamples = append(failSamples, filepath.Base(path)+": "+importErr.Error())
			}
			return nil
		}
		s.ok++
		s.contacts += len(result.Contacts)
		s.rejected += len(result.Rejected)
		if distinctByFmt[format] == nil {
			distinctByFmt[format] = map[string]struct{}{}
		}
		for _, id := range result.Contacts {
			distinctByFmt[format][id] = struct{}{}
		}
		for _, rej := range result.Rejected {
			reasons["rejected record: "+classifyDryRunError(rej.Reason)]++
		}
		return nil
	})
	if walkErr != nil {
		t.Fatalf("walk corpus: %v", walkErr)
	}

	t.Logf("corpus dry-run over %s", root)
	for _, format := range sortedStatKeys(byFmt) {
		s := byFmt[format]
		t.Logf("  %-18s files=%-6d ok=%-6d failed=%-6d skipped=%-5d contact_refs=%-7d DISTINCT=%-7d rejected_records=%d",
			format, s.files, s.ok, s.failed, s.skipped, s.contacts, len(distinctByFmt[format]), s.rejected)
	}
	t.Logf("failure reasons:")
	for _, r := range sortedCountKeys(reasons) {
		t.Logf("  %6d  %s", reasons[r], r)
	}
	for _, sample := range failSamples {
		t.Logf("  sample: %s", truncateDryRun(sample, 160))
	}
}

// classifyDryRunError reduces an import error to a stable bucket so failures
// aggregate instead of fragmenting on per-file specifics.
func classifyDryRunError(message string) string {
	switch {
	case strings.Contains(message, "unsupported vCard property"):
		return "unsupported vCard property"
	case strings.Contains(message, "unsupported vCard parameter"):
		return "unsupported vCard parameter"
	case strings.Contains(message, "unsupported CSV field"):
		return "unsupported CSV field"
	case strings.Contains(message, "unsupported Apple"):
		return "unsupported Apple field"
	case strings.Contains(message, "unsupported GEDCOM"):
		return "unsupported GEDCOM tag"
	case strings.Contains(message, "has no contacts"):
		return "no contacts detected"
	case strings.Contains(message, "unterminated vCard"):
		return "unterminated vCard"
	default:
		return truncateDryRun(message, 60)
	}
}

func truncateDryRun(value string, limit int) string {
	if len(value) <= limit {
		return value
	}

	return value[:limit] + "…"
}

func sortedStatKeys[V any](m map[string]V) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	return keys
}

func sortedCountKeys(m map[string]int) []string {
	keys := make([]string, 0, len(m))
	for key := range m {
		keys = append(keys, key)
	}
	sort.Slice(keys, func(i, j int) bool {
		if m[keys[i]] != m[keys[j]] {
			return m[keys[i]] > m[keys[j]]
		}

		return keys[i] < keys[j]
	})

	return keys
}
