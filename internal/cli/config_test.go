// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestAdminConfigValidate(t *testing.T) {
	path, key := writeEncryptedCLIConfig(t)
	t.Setenv("GMEOW_SOPS_UNLOCK_KEY", key)
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", path, "config", "validate"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(out.String(), "config valid") {
		t.Fatalf("unexpected output: %s", out.String())
	}
}

func TestAdminSecretListHidesValues(t *testing.T) {
	path, key := writeEncryptedCLIConfig(t)
	t.Setenv("GMEOW_SOPS_UNLOCK_KEY", key)
	var out bytes.Buffer
	command := NewAdminCommand(&out, strings.NewReader(""))
	command.SetArgs([]string{"--config", path, "config", "secret", "list"})

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	output := out.String()
	if !strings.Contains(output, "postgres_password references=1") {
		t.Fatalf("unexpected output: %s", output)
	}
	if strings.Contains(output, "postgres-password") {
		t.Fatalf("secret value leaked in output: %s", output)
	}
}

func TestAdminSecretSetIsValidatedStub(t *testing.T) {
	path, key := writeEncryptedCLIConfig(t)
	t.Setenv("GMEOW_SOPS_UNLOCK_KEY", key)
	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader("new-value\n"))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "set", "postgres_password"},
	)

	err := command.Execute()
	if err == nil {
		t.Fatal("expected Phase 00 stub error")
	}
	if !strings.Contains(err.Error(), "Phase 00") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestAdminSecretUnsetRefusesReferencedSecret(t *testing.T) {
	path, key := writeEncryptedCLIConfig(t)
	t.Setenv("GMEOW_SOPS_UNLOCK_KEY", key)
	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader(""))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "unset", "postgres_password"},
	)

	err := command.Execute()
	if err == nil {
		t.Fatal("expected referenced secret error")
	}
	if !strings.Contains(err.Error(), "still referenced") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func writeEncryptedCLIConfig(t *testing.T) (string, string) {
	t.Helper()
	requireCommand(t, "sops")
	requireCommand(t, "age-keygen")
	keyPath := filepath.Join(t.TempDir(), "key.txt")
	output, err := exec.Command("age-keygen", "-o", keyPath).CombinedOutput()
	if err != nil {
		t.Fatalf("generate age key: %v: %s", err, output)
	}
	recipient := regexp.MustCompile(`age1[0-9a-z]+`).FindString(string(output))
	if recipient == "" {
		t.Fatalf("age-keygen output did not include public recipient: %s", output)
	}
	plainPath := filepath.Join(t.TempDir(), "gmeow.toml")
	body := `
[system]
config_version = 1
instance_id = "test"
data_dir = "data"

[filestore]
root = "data/filestore"

[postgres]
enabled = true
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
password_secret = "postgres_password"
ssl_mode = "disable"

[secrets]
postgres_password = "postgres-password"
`
	if err := os.WriteFile(plainPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	encrypted, err := exec.Command("sops", "encrypt", "--input-type", "binary", "--output-type", "binary", "--age", recipient, plainPath).
		Output()
	if err != nil {
		t.Fatalf("encrypt config: %v", err)
	}
	encryptedPath := filepath.Join(t.TempDir(), "gmeow.sops.toml")
	if err := os.WriteFile(encryptedPath, encrypted, 0o600); err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(keyPath)
	if err != nil {
		t.Fatal(err)
	}
	return encryptedPath, string(key)
}

func requireCommand(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s CLI is not installed", name)
	}
}
