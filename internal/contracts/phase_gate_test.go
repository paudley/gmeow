// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import (
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

func TestTransitionalPythonSyncDoesNotOwnAnalysisScheduling(t *testing.T) {
	content, err := os.ReadFile(filepath.Join(repoRoot(t), "src", "gmeow", "sync.py"))
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

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
