// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"os"
	"path/filepath"
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
	configPath := repoConfigPath(t)
	home := t.TempDir()
	keyPath := filepath.Join(home, ".config", "gmeow", "key.txt")
	if err := os.MkdirAll(filepath.Dir(keyPath), 0o700); err != nil {
		t.Fatal(err)
	}
	key, err := os.ReadFile(
		filepath.Join(os.Getenv("HOME"), ".config", "gmeow", "key.txt"),
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(keyPath, key, 0o600); err != nil {
		t.Fatal(err)
	}
	t.Setenv(unlockEnvName, "")
	t.Setenv(configEnvName, "")
	t.Setenv("HOME", home)

	loaded, err := Load(Options{Path: configPath})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Config.System.InstanceID == "" {
		t.Fatalf("unexpected instance: %s", loaded.Config.System.InstanceID)
	}
}

func TestLoadUsesConfigEnvironmentFallback(t *testing.T) {
	configPath := repoConfigPath(t)
	t.Setenv(configEnvName, configPath)

	loaded, err := Load(Options{})
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

func TestLoadRejectsRabbitMQEnabledSwitch(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(
			minimalConfig(),
			"[rabbitmq]\n",
			"[rabbitmq]\nenabled = false\n",
			1,
		),
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected RabbitMQ enabled switch to fail")
	}
	if !strings.Contains(err.Error(), "rabbitmq.enabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsSchedulerQueuePrefixOutsideGmeowNamespace(t *testing.T) {
	configPath := writeConfig(
		t,
		minimalConfig()+"\n[scheduler]\nqueue_prefix = \"other:\"\n",
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected scheduler queue prefix validation error")
	}
	if !strings.Contains(err.Error(), "gmeow.") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsWrongRabbitMQVHost(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(
			minimalConfig(),
			"vhost = \"gmeow\"",
			"vhost = \"/\"",
			1,
		),
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected RabbitMQ vhost validation error")
	}
	if !strings.Contains(err.Error(), "gmeow") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSourceCapabilitiesByKind(t *testing.T) {
	valid := Config{
		System: SystemConfig{
			ConfigVersion: currentConfigVersion,
			InstanceID:    "test",
			DataDir:       "data",
		},
		Filestore: FilestoreConfig{Root: "data/filestore"},
		Postgres: PostgresConfig{
			Host:     "127.0.0.1",
			Port:     5432,
			Database: "gmeow",
			User:     "gmeow",
		},
		RabbitMQ: RabbitMQConfig{
			Host:      "127.0.0.1",
			Port:      5672,
			User:      "gmeow",
			VHost:     "gmeow",
			TestUser:  "gmeow-test",
			TestVHost: "gmeow-test",
		},
		Sources: []SourceConfig{{
			Name:         "primary",
			Kind:         "gmail",
			Capabilities: []string{"hydrate", "live_search", "actions"},
		}},
	}
	if err := validateConfig(valid); err != nil {
		t.Fatal(err)
	}

	invalid := valid
	invalid.Sources = []SourceConfig{{
		Name:         "drive",
		Kind:         "drive",
		Capabilities: []string{"live_search"},
	}}

	err := validateConfig(invalid)
	if err == nil {
		t.Fatal("expected unsupported Drive capability to fail")
	}
	if !strings.Contains(err.Error(), "does not support capability") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadDefaultsRPCUnixSocketEndpoints(t *testing.T) {
	configPath := writeConfig(t, minimalConfig())
	t.Setenv(unlockEnvName, "test-key")

	loaded, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected plaintext secret rejection before dependency checks")
	}
	if !strings.Contains(err.Error(), "SOPS-encrypted leaf") {
		t.Fatalf("unexpected precondition error: %v", err)
	}

	resolved := resolvedRPC(RPCConfig{})
	if resolved.Filestore.Network != "unix" ||
		resolved.Filestore.Address != "/run/gmeow/filestore.sock" ||
		resolved.Scheduler.Address != "/run/gmeow/scheduler.sock" ||
		resolved.Query.Address != "/run/gmeow/query.sock" {
		t.Fatalf("unexpected default RPC endpoints: %#v", resolved)
	}
	_ = loaded
}

func TestValidateRPCRejectsNonLoopbackTCP(t *testing.T) {
	err := validateRPCEndpoint("rpc.filestore", RPCEndpointConfig{
		Network: "tcp",
		Address: "0.0.0.0:9010",
	})
	if err == nil {
		t.Fatal("expected non-loopback TCP bind to fail")
	}
	if !strings.Contains(err.Error(), "loopback") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadRejectsUnknownWholeSecretReferences(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(
			minimalConfig(),
			"host = \"127.0.0.1\"",
			"host = \"127.0.0.1\"\ndsn_secret = \"postgres_dsn\"",
			1,
		),
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
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
	if !strings.Contains(err.Error(), "SOPS-encrypted leaf") {
		t.Fatalf("expected SOPS leaf protection error, got %v", err)
	}
}

func TestLoadRejectsOpaqueWholeFileSOPSConfig(t *testing.T) {
	configPath := writeConfig(t, `{"data":"ENC[AES256_GCM,data:fixture]","sops":{}}`)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected whole-file SOPS config rejection")
	}
	if !strings.Contains(err.Error(), "whole-file SOPS encryption is not supported") {
		t.Fatalf("expected whole-file SOPS error, got %v", err)
	}
}

func TestLoadAcceptsSOPSEncryptedPasswordLeaves(t *testing.T) {
	loaded, err := Load(Options{Path: repoConfigPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Resolved.Postgres.Password == "" {
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
		loaded.SecretReferences["rabbitmq_password"] != 1 ||
		loaded.SecretReferences["rabbitmq_test_password"] != 1 {
		t.Fatalf(
			"expected only named leaf secret references, got %#v",
			loaded.SecretReferences,
		)
	}
}

func TestLoadResolvesReferencedSecrets(t *testing.T) {
	loaded, err := Load(Options{Path: repoConfigPath(t)})
	if err != nil {
		t.Fatal(err)
	}
	if loaded.Resolved.Postgres.Password == "" {
		t.Fatalf("postgres password was not resolved")
	}
	if !strings.Contains(loaded.Resolved.RabbitMQ.URL, "@127.0.0.1:5672/gmeow") {
		t.Fatalf("rabbitmq URL was not resolved correctly")
	}
	if !strings.Contains(loaded.Resolved.RabbitMQ.TestURL, "@127.0.0.1:5672/gmeow-test") {
		t.Fatalf("rabbitmq test URL was not resolved correctly")
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

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected missing secret error")
	}
	if !strings.Contains(err.Error(), "postgres_password") {
		t.Fatalf("expected missing secret name, got %v", err)
	}
}

func writeConfig(t *testing.T, body string) string {
	t.Helper()
	path := filepath.Join(t.TempDir(), "gmeow.toml")
	if err := os.WriteFile(path, []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func repoConfigPath(t *testing.T) string {
	t.Helper()
	path := filepath.Clean(filepath.Join("..", "..", "gmeow.toml"))
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("repo config is required for live config tests: %v", err)
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
host = "127.0.0.1"
port = 5432
database = "gmeow"
user = "gmeow"
ssl_mode = "disable"

[rabbitmq]
host = "127.0.0.1"
port = 5672
user = "user"
vhost = "gmeow"
test_user = "test-user"
test_vhost = "gmeow-test"

[secrets]
postgres_password = "postgres-password"
rabbitmq_password = "password"
rabbitmq_test_password = "test-rabbit-password"

[[interfaces]]
name = "rest"
kind = "rest"
host = "127.0.0.1"
port = 8765

[[analysis.analyzers]]
name = "noop"
version = "phase00"
worker_kind = "python"
`
}
