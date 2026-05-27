// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package config

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/BurntSushi/toml"
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

func TestLoadRejectsNegativeAnalysisWorkerConcurrency(t *testing.T) {
	configPath := writeConfig(
		t,
		minimalConfig()+"\n[analysis]\nworker_concurrency = -1\n",
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected worker concurrency validation error")
	}
	if !strings.Contains(err.Error(), "analysis.worker_concurrency") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestValidateSourceCapabilitiesByKind(t *testing.T) {
	valid := validConfig()
	valid.Sources = []SourceConfig{{
		Name:         "primary",
		Kind:         "gmail",
		Capabilities: []string{"hydrate", "live_search", "actions"},
	}}
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

func TestValidateGmailBackfillAndInboxRefreshFlags(t *testing.T) {
	valid := validConfig()
	valid.Sources = []SourceConfig{{
		Name:         "primary",
		Kind:         "gmail",
		Capabilities: []string{"backfill", "hydrate", "live_search", "actions"},
		Backfill: SourceBackfillConfig{
			Enabled: true,
			Query:   "newer_than:30d",
			Resume:  true,
		},
		InboxRefresh: SourceInboxRefreshConfig{
			Enabled:  true,
			Query:    "in:inbox newer_than:30d",
			Interval: "5m",
		},
	}}
	if err := validateConfig(valid); err != nil {
		t.Fatal(err)
	}

	invalid := valid
	invalid.Sources = []SourceConfig{{
		Name:         "primary",
		Kind:         "gmail",
		Capabilities: []string{"hydrate", "live_search"},
		Backfill:     SourceBackfillConfig{Enabled: true},
	}}
	err := validateConfig(invalid)
	if err == nil || !strings.Contains(err.Error(), "requires backfill capability") {
		t.Fatalf("expected backfill capability validation, got %v", err)
	}
}

func TestResolvedSourceDefaultsToUnixEndpoint(t *testing.T) {
	parsed := validConfig()
	parsed.Secrets = map[string]string{
		postgresPasswordName: "postgres",
		rabbitPasswordName:   "rabbit",
		testRabbitPassName:   "test-rabbit",
	}
	parsed.Sources = []SourceConfig{{
		Name:         "primary",
		Kind:         "gmail",
		Capabilities: []string{"backfill"},
	}}
	resolved, err := resolveSecrets(parsed, secretReferences(parsed))
	if err != nil {
		t.Fatal(err)
	}
	if len(resolved.Sources) != 1 ||
		resolved.Sources[0].Endpoint.Network != "unix" ||
		resolved.Sources[0].Endpoint.Address != "/run/gmeow/source-primary.sock" {
		t.Fatalf("unexpected source endpoint: %#v", resolved.Sources)
	}
}

func TestValidateInterfaceConfig(t *testing.T) {
	valid := validConfig()
	valid.Interfaces = []InterfaceConfig{
		{Name: "mcp", Kind: "mcp"},
		{
			Name:           "mcp-http",
			Kind:           "mcp",
			Host:           "127.0.0.1",
			Port:           9876,
			SessionTimeout: "30m",
		},
		{Name: "rest", Kind: "rest", Host: "127.0.0.1", Port: 8765},
		{
			Name:     "imap",
			Kind:     "imap",
			Host:     "127.0.0.1",
			Port:     1143,
			Username: "reader",
			Facets:   []string{"mail_message"},
		},
	}
	if err := validateConfig(valid); err != nil {
		t.Fatal(err)
	}

	cases := map[string]InterfaceConfig{
		"unsupported kind": {Name: "bad", Kind: "smtp", Host: "127.0.0.1", Port: 2525},
		"non-loopback":     {Name: "rest", Kind: "rest", Host: "0.0.0.0", Port: 8765},
		"mcp non-loopback": {Name: "mcp-http", Kind: "mcp", Host: "0.0.0.0", Port: 9876},
		"mcp timeout": {
			Name:           "mcp-http",
			Kind:           "mcp",
			Host:           "127.0.0.1",
			Port:           9876,
			SessionTimeout: "bogus",
		},
		"imap username": {
			Name:   "imap",
			Kind:   "imap",
			Host:   "127.0.0.1",
			Port:   1143,
			Facets: []string{"mail_message"},
		},
		"imap facet": {
			Name:     "imap",
			Kind:     "imap",
			Host:     "127.0.0.1",
			Port:     1143,
			Username: "reader",
			Facets:   []string{"file"},
		},
	}
	for name, iface := range cases {
		t.Run(name, func(t *testing.T) {
			invalid := validConfig()
			invalid.Interfaces = []InterfaceConfig{iface}
			if err := validateConfig(invalid); err == nil {
				t.Fatal("expected invalid interface to fail")
			}
		})
	}
}

func TestSourceCredentialSecretIsReferenced(t *testing.T) {
	parsed := Config{
		Sources: []SourceConfig{{
			Name:             "primary",
			Kind:             "gmail",
			CredentialSecret: "gmail_credentials_json",
		}},
	}
	references := secretReferences(parsed)
	if references["gmail_credentials_json"] != 1 {
		t.Fatalf("expected gmail credential secret reference, got %#v", references)
	}
}

func TestLoadAcceptsGmailDelegatedSubject(t *testing.T) {
	configPath := writeConfig(
		t,
		strings.Replace(
			minimalConfig(),
			`rabbitmq_test_password = "test-rabbit-password"`,
			`rabbitmq_test_password = "test-rabbit-password"
gmail_credentials_json = "credentials"`,
			1,
		)+`
[[sources]]
name = "primary"
kind = "gmail"
credential_secret = "gmail_credentials_json"
user_id = "me"
delegated_subject = "paudley@blackcat.ca"
capabilities = ["hydrate", "live_search", "live_retrieve", "actions"]
`,
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected plaintext secret rejection before dependency checks")
	}
	if !strings.Contains(err.Error(), "SOPS-encrypted leaf") {
		t.Fatalf(
			"expected delegated source config to decode before secret checks, got %v",
			err,
		)
	}
}

func TestLoadRejectsUnknownSourceKey(t *testing.T) {
	configPath := writeConfig(
		t,
		minimalConfig()+`
[[sources]]
name = "primary"
kind = "gmail"
unexpected = "value"
`,
	)
	t.Setenv(unlockEnvName, "test-key")

	_, err := Load(Options{Path: configPath})
	if err == nil {
		t.Fatal("expected unknown source key error")
	}
	if !strings.Contains(err.Error(), "sources.unexpected") {
		t.Fatalf("expected unknown source key detail, got %v", err)
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
	if strings.TrimSpace(loaded.Config.Postgres.Host) == "" ||
		strings.TrimSpace(loaded.Config.Postgres.Database) == "" ||
		strings.TrimSpace(loaded.Config.Postgres.User) == "" {
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
	if len(loaded.Resolved.Worker.Analyzers) == 0 {
		t.Fatalf(
			"expected resolved worker analyzer payload, got %#v",
			loaded.Resolved.Worker.Analyzers,
		)
	}
	if loaded.Resolved.Worker.Analyzers[0].Name != "text.extract" {
		t.Fatalf(
			"expected production analyzer payload, got %#v",
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

func validConfig() Config {
	return Config{
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
	}
}

func repoConfigPath(t *testing.T) string {
	t.Helper()
	path := strings.TrimSpace(os.Getenv("GMEOW_TEST_CONFIG"))
	if path == "" {
		t.Skip("GMEOW_TEST_CONFIG is required for live config tests")
	}
	if _, err := os.Stat(path); err != nil {
		t.Fatalf("test config is required for live config tests: %v", err)
	}
	requireTestConfigFile(t, path)
	return path
}

func requireTestConfigFile(t *testing.T, path string) {
	t.Helper()
	var parsed struct {
		Postgres PostgresConfig `toml:"postgres"`
		RabbitMQ RabbitMQConfig `toml:"rabbitmq"`
	}
	if _, err := toml.DecodeFile(path, &parsed); err != nil {
		t.Fatalf("decode test config guard: %v", err)
	}
	if !strings.Contains(parsed.Postgres.Database, "test") ||
		!strings.Contains(parsed.Postgres.User, "test") {
		t.Fatalf(
			"refusing live config test against non-test postgres target database=%q user=%q",
			parsed.Postgres.Database,
			parsed.Postgres.User,
		)
	}
	if !strings.Contains(parsed.RabbitMQ.VHost, "test") ||
		!strings.Contains(parsed.RabbitMQ.User, "test") {
		t.Fatalf(
			"refusing live config test against non-test rabbitmq target vhost=%q user=%q",
			parsed.RabbitMQ.VHost,
			parsed.RabbitMQ.User,
		)
	}
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
