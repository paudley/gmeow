// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"blackat.ca/gmeow/internal/contracts"
	"blackat.ca/gmeow/internal/filestore"
)

func TestAdminFilestoreVerifyReportsCleanStore(t *testing.T) {
	root := filepath.Join(t.TempDir(), "filestore")
	configPath := writePlainCLIConfig(t, root)
	t.Setenv("GMEOW_SOPS_UNLOCK_KEY", "test-key")
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", configPath, "filestore", "verify"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "status=ok") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestAdminFilestoreVerifyResolvesRelativeRootFromConfigPath(t *testing.T) {
	configDir := t.TempDir()
	root := filepath.Join(configDir, "data", "filestore")
	store := filestore.NewFilesystemStore(root)
	if _, err := store.Put(context.Background(), filestore.PutRequest{
		Reader: strings.NewReader("hello"),
		Facets: []contracts.Facet{{Kind: "file"}},
	}); err != nil {
		t.Fatal(err)
	}
	configPath := writePlainCLIConfigAt(t, configDir, "data/filestore")
	t.Setenv("GMEOW_SOPS_UNLOCK_KEY", "test-key")
	originalWD, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := os.Chdir(originalWD); err != nil {
			t.Fatalf("restore working directory: %v", err)
		}
	})
	if err := os.Chdir(t.TempDir()); err != nil {
		t.Fatal(err)
	}
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", configPath, "filestore", "verify"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "status=ok checked=1") {
		t.Fatalf("relative root was not resolved from config path: %s", out.String())
	}
}

func writePlainCLIConfig(t *testing.T, filestoreRoot string) string {
	t.Helper()
	return writePlainCLIConfigAt(t, t.TempDir(), filestoreRoot)
}

func writePlainCLIConfigAt(t *testing.T, dir, filestoreRoot string) string {
	t.Helper()
	path := filepath.Join(dir, "gmeow.toml")
	body := `
[system]
config_version = 1
instance_id = "test"
data_dir = "data"

[filestore]
root = "` + filestoreRoot + `"

[postgres]
enabled = false
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
password_secret = ""
ssl_mode = "disable"

[rabbitmq]
enabled = false
url_secret = ""

[secrets]
`
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}
