// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestGoImportsUseDeclaredModulePath(t *testing.T) {
	root := repoRoot(t)
	misspelled := "black" + "at.ca/gmeow"
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			switch entry.Name() {
			case ".git", "coding-ethos":
				return filepath.SkipDir
			}
			return nil
		}
		if filepath.Ext(path) != ".go" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), misspelled) {
			t.Fatalf("%s imports misspelled module path %s", path, misspelled)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestFilestoreDoesNotImportQueryOrPostgres(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "filestore")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		for _, forbidden := range []string{
			"internal/query",
			"jackc/pgx",
			"database/sql",
			"pressly/goose",
		} {
			if strings.Contains(text, forbidden) {
				t.Fatalf("%s imports forbidden dependency %q", path, forbidden)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAnalysisRuntimeDoesNotImportConcreteFilestore(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "analysis")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if strings.Contains(string(content), "internal/filestore") {
			t.Fatalf(
				"%s imports concrete FILESTORE; ANALYSIS must use FilestoreService gRPC",
				path,
			)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestSourceRuntimeUsesRPCFilestoreBoundary(t *testing.T) {
	root := filepath.Join(repoRoot(t), "internal", "source")
	err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || filepath.Ext(path) != ".go" ||
			strings.HasSuffix(path, "_test.go") {
			return nil
		}
		content, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(content)
		if strings.Contains(text, "internal/filestore") {
			t.Fatalf(
				"%s imports concrete FILESTORE; SOURCE must use FilestoreService gRPC",
				path,
			)
		}
		for _, required := range []string{
			"LookupSourceObject",
			"TryAcquireSourceIngest",
		} {
			if path == filepath.Join(root, "interfaces.go") &&
				!strings.Contains(text, required) {
				t.Fatalf("SOURCE service contract is missing %s", required)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestPhaseZeroToThreePythonRetiredPathsStayRetired(t *testing.T) {
	root := repoRoot(t)
	retired := []string{
		"src/gmeow/config.py",
		"src/gmeow/object_store.py",
		"src/gmeow/db.py",
		"src/gmeow/pg_cache.py",
		"src/gmeow/pg_cache_archive.py",
		"src/gmeow/pg_cache_helpers.py",
		"src/gmeow/pg_cache_imap.py",
		"src/gmeow/pg_cache_jobs.py",
		"src/gmeow/semantic_pg.py",
		"src/gmeow/maintenance.py",
		"tests/_test_config.py",
	}
	for _, relative := range retired {
		if _, err := os.Stat(filepath.Join(root, relative)); err == nil {
			t.Fatalf("retired phase 0-3 Python path is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat retired path %s: %v", relative, err)
		}
	}
}

func TestPhaseFourToFivePythonRetiredPathsStayRetired(t *testing.T) {
	root := repoRoot(t)
	retired := []string{
		"src/gmeow/attachment_analysis.py",
		"src/gmeow/categories.py",
		"src/gmeow/semantic.py",
		"src/gmeow/text_index.py",
		"src/gmeow/headers.py",
		"src/gmeow/gmail.py",
		"src/gmeow/sync.py",
		"src/gmeow/sync_backfill.py",
		"src/gmeow/gmail_actions.py",
		"src/gmeow/provision.py",
		"tests/test_gmail_actions.py",
		"tests/test_sync_backfill.py",
	}
	for _, relative := range retired {
		if _, err := os.Stat(filepath.Join(root, relative)); err == nil {
			t.Fatalf("retired phase 4-5 Python path is present: %s", relative)
		} else if !os.IsNotExist(err) {
			t.Fatalf("stat retired path %s: %v", relative, err)
		}
	}
}

func TestTransitionalPythonSyncDoesNotOwnAnalysisScheduling(t *testing.T) {
	path := filepath.Join(repoRoot(t), "src", "gmeow", "sync.py")
	content, err := os.ReadFile(path)
	if errors.Is(err, os.ErrNotExist) {
		return
	}
	if err != nil {
		t.Fatal(err)
	}
	for _, forbidden := range []string{
		"_enqueue_missing_message_analysis",
		"maintenance_scheduler",
	} {
		if strings.Contains(string(content), forbidden) {
			t.Fatalf("transitional Python sync still contains scheduler path %q", forbidden)
		}
	}
}

func TestDocsDoNotReintroduceGmailRawPayloadDuplication(t *testing.T) {
	for _, relative := range []string{
		"docs/GO_MIGRATION.md",
		"docs/GO_PHASE_05_SOURCE.md",
	} {
		content, err := os.ReadFile(filepath.Join(repoRoot(t), relative))
		if err != nil {
			t.Fatal(err)
		}
		for _, forbidden := range []string{
			"raw_rfc822",
			"body/raw/attachment",
		} {
			if strings.Contains(string(content), forbidden) {
				t.Fatalf(
					"%s reintroduced duplicate Gmail raw payload wording %q",
					relative,
					forbidden,
				)
			}
		}
	}
}

func TestPublicDocsDescribePhaseZeroThroughFiveRuntime(t *testing.T) {
	root := repoRoot(t)
	readme, err := os.ReadFile(filepath.Join(root, "README.md"))
	if err != nil {
		t.Fatal(err)
	}
	text := string(readme)
	for _, required := range []string{
		"Phases 0-5",
		"Go ANALYSIS worker runtime",
		"Go SOURCE adapters",
		"Gmail SOURCE adapter",
		"gmeow-intel",
	} {
		if !strings.Contains(text, required) {
			t.Fatalf(
				"README.md does not describe completed phase 0-5 runtime surface %q",
				required,
			)
		}
	}
	for _, stale := range []string{
		"Phases 0-3 provide",
		"Gmail SOURCE adapters, and ANALYSIS workers return in later Go migration phases",
		"Gmail provisioning and source runtime behavior move in later Go migration phases",
	} {
		if strings.Contains(text, stale) {
			t.Fatalf("README.md still contains stale phase wording %q", stale)
		}
	}
}

func TestPythonIntelDocsDescribeExternalAnalyzerAdapters(t *testing.T) {
	root := repoRoot(t)
	for _, relative := range []string{
		"python/gmeow_intel/__init__.py",
		"python/gmeow_intel/analyzers/MODULE.md",
		"python/gmeow_intel/analyzers/__init__.py",
		"python/tests/MODULE.md",
		"python/tests/__init__.py",
		"python/tests/test_contracts.py",
		"docs/SOURCE_DOCS.md",
	} {
		content, err := os.ReadFile(filepath.Join(root, relative))
		if err != nil {
			t.Fatal(err)
		}
		for _, stale := range []string{
			"Phase 00 keeps",
			"Phase 00 modules are placeholders",
			"Phase 00 `gmeow-intel` package skeleton",
			"Phase 00 only defines contracts",
			"later ANALYSIS work",
		} {
			if strings.Contains(string(content), stale) {
				t.Fatalf("%s still contains stale external analyzer wording %q", relative, stale)
			}
		}
	}
}

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
