// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"

	"blackcat.ca/gmeow/internal/config"
)

func TestAdminConfigValidate(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
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
	path := writeEncryptedCLIConfig(t)
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

func TestAdminSecretSetUpdatesEncryptedLeaf(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader("gmeow\n"))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "set", "postgres_password"},
	)

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "gmeow\n") {
		t.Fatal("secret value was written in plaintext")
	}
	if _, err := config.Load(config.Options{Path: path}); err != nil {
		t.Fatalf("updated config should validate: %v", err)
	}
}

func TestAdminSecretUnsetRefusesReferencedSecret(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
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

func TestAdminSecretUnsetRemovesUnreferencedSecretAtomically(t *testing.T) {
	path := writeEncryptedCLIConfig(t)
	file, err := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := file.WriteString("unused_password = '''\nunused\n'''\n"); err != nil {
		_ = file.Close()
		t.Fatal(err)
	}
	if err := file.Close(); err != nil {
		t.Fatal(err)
	}

	command := NewAdminCommand(&bytes.Buffer{}, strings.NewReader(""))
	command.SetArgs(
		[]string{"--config", path, "config", "secret", "unset", "unused_password"},
	)

	if err := command.Execute(); err != nil {
		t.Fatal(err)
	}
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(body), "unused_password") {
		t.Fatalf("unreferenced secret was not removed:\n%s", string(body))
	}
	if _, err := config.Load(config.Options{Path: path}); err != nil {
		t.Fatalf("updated config should validate: %v", err)
	}
}

func writeEncryptedCLIConfig(t *testing.T) string {
	t.Helper()
	plainPath := filepath.Join(t.TempDir(), "gmeow.toml")
	postgresPassword := localSecretLeaf(t, "postgres_password")
	rabbitPassword := localSecretLeaf(t, "rabbitmq_password")
	testRabbitPassword := localSecretLeaf(t, "rabbitmq_test_password")
	body := `
[system]
config_version = 1
instance_id = "test"
data_dir = "data"

[filestore]
root = "data/filestore"

[postgres]
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
ssl_mode = "require"

[rabbitmq]
host = "127.0.0.1"
port = 5672
user = "gmeow"
vhost = "gmeow"
test_user = "gmeow-test"
test_vhost = "gmeow-test"

[secrets]
postgres_password = ` + tomlLiteralForCLIConfig(postgresPassword) + `
rabbitmq_password = ` + tomlLiteralForCLIConfig(rabbitPassword) + `
rabbitmq_test_password = ` + tomlLiteralForCLIConfig(testRabbitPassword) + `
`
	if err := os.WriteFile(plainPath, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return plainPath
}

func tomlLiteralForCLIConfig(value string) string {
	return "'''\n" + value + "\n'''"
}

func localSecretLeaf(t *testing.T, name string) string {
	t.Helper()
	var parsed struct {
		Secrets map[string]string `toml:"secrets"`
	}
	path := filepath.Clean(filepath.Join("..", "..", "gmeow.toml"))
	if _, err := toml.DecodeFile(path, &parsed); err != nil {
		t.Fatalf("decode local config for encrypted secret leaf: %v", err)
	}
	value := strings.TrimSpace(parsed.Secrets[name])
	if value == "" {
		t.Fatalf("local config missing encrypted secret leaf %q", name)
	}
	return value
}
