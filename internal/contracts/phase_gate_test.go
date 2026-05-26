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

func repoRoot(t *testing.T) string {
	t.Helper()
	root, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	return root
}
