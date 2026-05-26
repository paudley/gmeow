// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

func TestLoadFailsWithoutUnlockKey(t *testing.T) {
	configPath := writeConfig(t, minimalConfig())
	t.Setenv(unlockEnvName, "")
	t.Setenv(configEnvName, "")
	t.Setenv("HOME", t.TempDir())

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected missing unlock key error")
	}
	if !strings.Contains(err.Error(), unlockEnvName) {
		t.Fatalf("expected unlock key error, got %v", err)
	}
}

func TestLoadUsesUnlockKeyFileFallback(t *testing.T) {
	configPath := writeConfig(t, minimalConfig())
	home := t.TempDir()
	keyPath := filepath.Join(home, ".config", "gmeow", "key.txt")
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, []byte("file-key\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(unlockEnvName, "")
	t.Setenv(configEnvName, "")
	t.Setenv("HOME", home)

	loaded, err := Load(Options{Path: configPath, allowPlaintextSecretsForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.System.InstanceID != "test" {
		t.Fatalf("unexpected instance: %s", loaded.Config.System.InstanceID)
	}
}

func TestLoadUsesConfigEnvironmentFallback(t *testing.T) {
	configPath := writeConfig(t, minimalConfig())
	t.Setenv(unlockEnvName, "test-key")
	t.Setenv(configEnvName, configPath)

	loaded, err := Load(Options{allowPlaintextSecretsForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Path != configPath {
		t.Fatalf("expected env config path %s, got %s", configPath, loaded.Path)
	}
}

func TestLoadRejectsForbiddenPointers(t *testing.T) {
	cases := map[string]string{
		"config.file": minimalConfig() + "\n[config]\nfile = \"secondary.toml\"\n",
		"secrets.file": strings.Replace(
			minimalConfig(),
			"[secrets]\n",
			"[secrets]\nfile = \"secrets.sops.yaml\"\n",
			1,
		),
		"secrets.sops_file": strings.Replace(
			minimalConfig(),
			"[secrets]\n",
			"[secrets]\nsops_file = \"secrets.sops.yaml\"\n",
			1,
		),
	}
	for forbidden, body := range cases {
		t.Run(forbidden, func(t *testing.T) {
			configPath := writeConfig(t, body)
			t.Setenv(unlockEnvName, "test-key")

			_, err := Load(Options{Path: configPath})
			if err == nil {
				t.Fatal("expected forbidden pointer error")
			}
			if !strings.Contains(err.Error(), forbidden) {
				t.Fatalf("expected forbidden pointer detail %q, got %v", forbidden, err)
			}
		})
	}
}

func TestLoadRejectsUnknownConfigVersion(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(minimalConfig(), "config_version = 1", "config_version = 99", 1),
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected unsupported config version")
	}
}

func TestLoadRejectsSchedulerWithoutRabbitMQ(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(
			minimalConfig(),
			"[rabbitmq]\nenabled = true",
			"[rabbitmq]\nenabled = false",
			1,
		)+"\n[scheduler]\nenabled = true\nqueue_prefix = \"gmeow:\"\n",
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath, allowPlaintextSecretsForTests: true})
	if err == nil {
		t.Fatal("expected scheduler to require RabbitMQ")
	}
	if !strings.Contains(err.Error(), "rabbitmq.enabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsSchedulerQueuePrefixOutsideGmeowNamespace(t *testing.T) {
	configPath := writeConfig(
		t,
		minimalConfig()+"\n[scheduler]\nenabled = true\nqueue_prefix = \"other:\"\n",
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath, allowPlaintextSecretsForTests: true})
	if err == nil {
		t.Fatal("expected scheduler queue prefix validation error")
	}
	if !strings.Contains(err.Error(), "gmeow:") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsUnknownWholeSecretReferences(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(
			minimalConfig(),
			"password_secret = \"postgres_password\"",
			"dsn_secret = \"postgres_dsn\"",
			1,
		),
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath, allowPlaintextSecretsForTests: true})
	if err == nil {
		t.Fatal("expected unknown key error")
	}
	if !strings.Contains(err.Error(), "postgres.dsn_secret") {
		t.Fatalf("expected unknown whole-secret key detail, got %v", err)
	}
}

func TestLoadRejectsPlaintextReferencedSecrets(t *testing.T) {
	configPath := writeConfig(t, minimalConfig())
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected plaintext secret rejection")
	}
	if !strings.Contains(err.Error(), "SOPS-protected config") {
		t.Fatalf("expected SOPS protection error, got %v", err)
	}
}

func TestLoadSOPSConfigRequiresSOPSCLI(t *testing.T) {
	if _, err := exec.LookPath("sops"); err != nil {
		t.Skip("sops CLI is not installed")
	}
	configPath := writeConfig(t, `
[sops]
version = "fixture"
`)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected decrypt failure for invalid SOPS fixture")
	}
	if !strings.Contains(err.Error(), "decrypt config") {
		t.Fatalf("expected decrypt error, got %v", err)
	}
}

func TestLoadAcceptsSOPSEncryptedLeafSecrets(t *testing.T) {
	requireCommand(t, "sops")
	requireCommand(t, "age-keygen")
	keyPath := filepath.Join(t.TempDir(), "key.txt")
	output, err := exec.Command("age-keygen", "-o", keyPath).CombinedOutput()
	if err != nil {
		t.Fatalf("generate age key: %v: %s", err, output)
	}
	recipient := publicAgeRecipient(t, string(output))
	plainPath := writeConfig(t, minimalConfig())
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
	t.Setenv(unlockEnvName, string(key))

	loaded, err := Load(Options{Path: encryptedPath})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Resolved.Postgres.Password != "postgres-password" {
		t.Fatalf("postgres password was not resolved from SOPS config")
	}
	if loaded.Config.Postgres.Host != "127.0.0.1" ||
		loaded.Config.Postgres.Database != "gmeow" ||
		loaded.Config.Postgres.User != "gmeow" {
		t.Fatalf(
			"postgres operational fields were not decoded separately from the password leaf",
		)
	}
	if loaded.SecretReferences["postgres_password"] != 1 ||
		loaded.SecretReferences["rabbitmq_url"] != 1 {
		t.Fatalf(
			"expected only named leaf secret references, got %#v",
			loaded.SecretReferences,
		)
	}
}

func TestLoadResolvesReferencedSecrets(t *testing.T) {
	configPath := writeConfig(t, minimalConfig())
	t.Setenv(unlockEnvName, "test-key")

	loaded, err := Load(Options{Path: configPath, allowPlaintextSecretsForTests: true})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Resolved.Postgres.Password != "postgres-password" {
		t.Fatalf("postgres password was not resolved")
	}
	if loaded.Resolved.RabbitMQ.URL != "amqp://guest:guest@127.0.0.1:5672/gmeow" {
		t.Fatalf("rabbitmq URL was not resolved")
	}
	if loaded.SecretReferences["postgres_password"] != 1 {
		t.Fatalf(
			"unexpected postgres secret reference count: %d",
			loaded.SecretReferences["postgres_password"],
		)
	}
	if len(loaded.Resolved.Worker.Analyzers) != 1 {
		t.Fatalf(
			"expected resolved worker analyzer payload, got %#v",
			loaded.Resolved.Worker.Analyzers,
		)
	}
}

func TestLoadRejectsMissingReferencedSecret(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(minimalConfig(), `postgres_password = "postgres-password"`, "", 1),
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath, allowPlaintextSecretsForTests: true})
	if err == nil {
		t.Fatal("expected missing secret error")
	}
	if !strings.Contains(err.Error(), "postgres_password") {
		t.Fatalf("expected missing secret name, got %v", err)
	}
}

func requireCommand(t *testing.T, name string) {
	t.Helper()
	if _, err := exec.LookPath(name); err != nil {
		t.Skipf("%s CLI is not installed", name)
	}
}

func publicAgeRecipient(t *testing.T, output string) string {
	t.Helper()
	match := regexp.MustCompile(`age1[0-9a-z]+`).FindString(output)
	if match == "" {
		t.Fatalf("age-keygen output did not include public recipient: %s", output)
	}
	return match
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gmeow.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func minimalConfig() string {
	return `
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

[rabbitmq]
enabled = true
url_secret = "rabbitmq_url"

[secrets]
postgres_password = "postgres-password"
rabbitmq_url = "amqp://guest:guest@127.0.0.1:5672/gmeow"

[[interfaces]]
name = "rest"
kind = "rest"
enabled = true
host = "127.0.0.1"
port = 8765

[[analysis.analyzers]]
name = "noop"
version = "phase00"
enabled = true
worker_kind = "python"
`
}
